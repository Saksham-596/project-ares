#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <condition_variable>
#include <cstring>
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <cerrno>
#include <cstdint>
#include <string>

// ==================================================
// CONFIGURATION
// ==================================================

constexpr std::size_t NUM_SHARDS = 16;
constexpr std::size_t SHARD_CAPACITY = 10000;

constexpr int NUM_WORKERS = 4;
constexpr int SERVER_PORT = 8080;
constexpr int LISTEN_BACKLOG = 1000;

constexpr int RECEIVE_TIMEOUT_SECONDS = 30;

constexpr std::size_t FRAME_HEADER_SIZE = 9;

// Maximum complete request frame.
// Prevents a client from forcing unbounded buffering/allocation.
constexpr std::size_t MAX_FRAME_SIZE = 1024 * 1024; // 1 MiB

// ==================================================
// SIGNAL / SHUTDOWN STATE
// ==================================================

volatile std::sig_atomic_t shutdown_requested = 0;

std::atomic<bool> keep_running{true};

int server_fd = -1;

// Signal handler performs only signal-safe work.
//
// We deliberately do not:
// - print
// - lock mutexes
// - allocate memory
// - access STL containers
//
// shutdown() is used to interrupt accept().

void signal_handler(int)
{
    shutdown_requested = 1;

    if (server_fd >= 0)
    {
        shutdown(server_fd, SHUT_RDWR);
    }
}

// ==================================================
// LRU NODE
// ==================================================

struct Node
{
    std::string key;
    std::string value;

    Node *prev;
    Node *next;

    Node(std::string k, std::string v)
        : key(std::move(k)),
          value(std::move(v)),
          prev(nullptr),
          next(nullptr)
    {
    }
};

// ==================================================
// LRU SHARD
// ==================================================

struct Shard
{
    std::size_t capacity = SHARD_CAPACITY;

    std::unordered_map<std::string, Node *> map;

    // head = MRU sentinel
    // tail = LRU sentinel
    //
    // head <-> MRU ... LRU <-> tail

    Node *head;
    Node *tail;

    std::mutex lock;

    Shard()
    {
        head = new Node("", "");
        tail = new Node("", "");

        head->next = tail;
        head->prev = nullptr;

        tail->prev = head;
        tail->next = nullptr;
    }

    ~Shard()
    {
        // map owns all real nodes.
        for (auto &entry : map)
        {
            delete entry.second;
        }

        delete head;
        delete tail;
    }

    Shard(const Shard &) = delete;
    Shard &operator=(const Shard &) = delete;

    // Remove node from current position.
    void removeNode(Node *node)
    {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    // Insert node immediately after head = MRU.
    void addHead(Node *node)
    {
        node->next = head->next;
        node->prev = head;

        head->next->prev = node;
        head->next = node;
    }
};

// ==================================================
// DATABASE
// ==================================================

std::vector<Shard> database_shard(NUM_SHARDS);

// ==================================================
// THREAD POOL
// ==================================================

std::queue<int> task_queue;

std::mutex queue_mutex;
std::condition_variable cv;

// ==================================================
// SEND ALL
// ==================================================
//
// TCP send() is allowed to perform a partial write.
// Keep sending until the complete response is transmitted.
//
// MSG_NOSIGNAL prevents a broken client connection from
// terminating the process with SIGPIPE on Linux/macOS.
//

bool send_all(
    int socket_fd,
    const char *data,
    std::size_t length)
{
    std::size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent = send(
            socket_fd,
            data + total_sent,
            length - total_sent,
            MSG_NOSIGNAL);

        if (sent > 0)
        {
            total_sent += static_cast<std::size_t>(sent);
            continue;
        }

        if (sent < 0 && errno == EINTR)
        {
            continue;
        }

        return false;
    }

    return true;
}

// ==================================================
// PROCESS ONE COMPLETE FRAME
// ==================================================
//
// Protocol:
//
// byte 0      : opcode
// bytes 1-4   : key length, network byte order
// bytes 5-8   : value length, network byte order
// bytes 9...  : key + value
//
// V1 intentionally keeps the original response semantics:
//
// SET success -> OK\n
// GET hit     -> OK\n
// GET miss    -> nil\n
// invalid     -> ERR\n
//

bool process_frame(
    int client_socket,
    const std::vector<std::uint8_t> &buffer,
    std::size_t frame_size)
{
    if (frame_size < FRAME_HEADER_SIZE ||
        frame_size > MAX_FRAME_SIZE)
    {
        const char response[] = "ERR\n";

        return send_all(
            client_socket,
            response,
            sizeof(response) - 1);
    }

    const std::uint8_t opcode = buffer[0];

    std::uint32_t key_len_network = 0;
    std::uint32_t value_len_network = 0;

    std::memcpy(
        &key_len_network,
        buffer.data() + 1,
        sizeof(key_len_network));

    std::memcpy(
        &value_len_network,
        buffer.data() + 5,
        sizeof(value_len_network));

    const std::size_t key_len =
        ntohl(key_len_network);

    const std::size_t value_len =
        ntohl(value_len_network);

    // The parser already performs these checks.
    // Keep them here as a defensive invariant.
    if (key_len + value_len >
        MAX_FRAME_SIZE - FRAME_HEADER_SIZE)
    {
        const char response[] = "ERR\n";

        return send_all(
            client_socket,
            response,
            sizeof(response) - 1);
    }

    if (key_len >
        frame_size - FRAME_HEADER_SIZE)
    {
        const char response[] = "ERR\n";

        return send_all(
            client_socket,
            response,
            sizeof(response) - 1);
    }

    if (value_len >
        frame_size - FRAME_HEADER_SIZE - key_len)
    {
        const char response[] = "ERR\n";

        return send_all(
            client_socket,
            response,
            sizeof(response) - 1);
    }

    // Explicit opcode validation.
    if (opcode != 0x01 && opcode != 0x02)
    {
        const char response[] = "ERR\n";

        return send_all(
            client_socket,
            response,
            sizeof(response) - 1);
    }

    // Construct key only after validating its size.
    std::string key(
        reinterpret_cast<const char *>(
            buffer.data() + FRAME_HEADER_SIZE),
        key_len);

    const std::size_t shard_index =
        std::hash<std::string>{}(key) % NUM_SHARDS;

    Shard &shard =
        database_shard[shard_index];

    const char *response = "ERR\n";
    std::size_t response_len = 4;

    {
        std::lock_guard<std::mutex> db_lock(
            shard.lock);

        auto it = shard.map.find(key);

        // ==================================================
        // SET
        // ==================================================

        if (opcode == 0x01)
        {
            std::string value(
                reinterpret_cast<const char *>(
                    buffer.data() +
                    FRAME_HEADER_SIZE +
                    key_len),
                value_len);

            if (it != shard.map.end())
            {
                // Existing key.
                Node *node = it->second;

                node->value = std::move(value);

                shard.removeNode(node);
                shard.addHead(node);
            }
            else
            {
                // New key.
                Node *node =
                    new Node(
                        std::move(key),
                        std::move(value));

                try
                {
                    shard.map.emplace(
                        node->key,
                        node);
                }
                catch (...)
                {
                    delete node;
                    throw;
                }

                shard.addHead(node);

                // Evict LRU if capacity exceeded.
                if (shard.map.size() >
                    shard.capacity)
                {
                    Node *lru =
                        shard.tail->prev;

                    shard.removeNode(lru);

                    shard.map.erase(lru->key);

                    delete lru;
                }
            }

            response = "OK\n";
            response_len = 3;
        }

        // ==================================================
        // GET
        // ==================================================

        else
        {
            if (it != shard.map.end())
            {
                Node *node = it->second;

                // GET makes the item MRU.
                shard.removeNode(node);
                shard.addHead(node);

                response = "OK\n";
                response_len = 3;
            }
            else
            {
                response = "nil\n";
                response_len = 4;
            }
        }
    }

    // Never hold the shard mutex while doing network I/O.
    return send_all(
        client_socket,
        response,
        response_len);
}

// ==================================================
// WORKER THREAD
// ==================================================

void worker_thread(int worker_id)
{
    std::cout
        << "[Worker "
        << worker_id
        << "] online and standing by...\n";

    while (keep_running)
    {
        int client_socket = -1;

        // --------------------------------------------------
        // Obtain a client from the queue.
        // --------------------------------------------------

        {
            std::unique_lock<std::mutex> lock(
                queue_mutex);

            cv.wait(
                lock,
                []
                {
                    return !task_queue.empty() ||
                           !keep_running.load();
                });

            if (!keep_running &&
                task_queue.empty())
            {
                break;
            }

            // During shutdown, do not start processing
            // newly queued work.
            if (!keep_running)
            {
                break;
            }

            client_socket =
                task_queue.front();

            task_queue.pop();
        }

        // --------------------------------------------------
        // Configure client socket.
        // --------------------------------------------------

        struct timeval tv{};

        tv.tv_sec = RECEIVE_TIMEOUT_SECONDS;
        tv.tv_usec = 0;

        if (setsockopt(
                client_socket,
                SOL_SOCKET,
                SO_RCVTIMEO,
                &tv,
                sizeof(tv)) < 0)
        {
            std::cerr
                << "[Warning] setsockopt(SO_RCVTIMEO) failed: "
                << std::strerror(errno)
                << '\n';

            close(client_socket);
            continue;
        }

        // --------------------------------------------------
        // TCP stream buffer.
        //
        // One read() does NOT necessarily equal one frame.
        // --------------------------------------------------

        std::vector<std::uint8_t> buffer;

        buffer.reserve(4096);

        bool client_ok = true;

        while (
            keep_running &&
            client_ok)
        {
            std::uint8_t temp_buffer[4096];

            ssize_t bytes_read =
                read(
                    client_socket,
                    temp_buffer,
                    sizeof(temp_buffer));

            // ==================================================
            // DATA
            // ==================================================

            if (bytes_read > 0)
            {
                const std::size_t bytes =
                    static_cast<std::size_t>(
                        bytes_read);

                // Never allow buffered data to exceed
                // the maximum legal frame size.
                if (buffer.size() >
                        MAX_FRAME_SIZE ||
                    bytes >
                        MAX_FRAME_SIZE -
                            buffer.size())
                {
                    std::cerr
                        << "[Warning] client exceeded "
                        << "maximum frame size\n";

                    break;
                }

                buffer.insert(
                    buffer.end(),
                    temp_buffer,
                    temp_buffer + bytes);

                // ==================================================
                // PARSE COMPLETE FRAMES
                // ==================================================

                while (true)
                {
                    // Not enough bytes for header.
                    if (buffer.size() <
                        FRAME_HEADER_SIZE)
                    {
                        break;
                    }

                    std::uint32_t key_len_network = 0;
                    std::uint32_t value_len_network = 0;

                    std::memcpy(
                        &key_len_network,
                        buffer.data() + 1,
                        sizeof(key_len_network));

                    std::memcpy(
                        &value_len_network,
                        buffer.data() + 5,
                        sizeof(value_len_network));

                    const std::size_t key_len =
                        ntohl(key_len_network);

                    const std::size_t value_len =
                        ntohl(value_len_network);

                    // --------------------------------------------------
                    // Safe frame-size calculation.
                    // --------------------------------------------------

                    if (key_len >
                            MAX_FRAME_SIZE -
                                FRAME_HEADER_SIZE ||
                        value_len >
                            MAX_FRAME_SIZE -
                                FRAME_HEADER_SIZE -
                                key_len)
                    {
                        std::cerr
                            << "[Warning] invalid frame size\n";

                        client_ok = false;
                        break;
                    }

                    const std::size_t total_frame_size =
                        FRAME_HEADER_SIZE +
                        key_len +
                        value_len;

                    // Incomplete frame.
                    if (buffer.size() <
                        total_frame_size)
                    {
                        break;
                    }

                    // --------------------------------------------------
                    // Process complete frame.
                    // --------------------------------------------------

                    if (!process_frame(
                            client_socket,
                            buffer,
                            total_frame_size))
                    {
                        client_ok = false;
                        break;
                    }

                    // --------------------------------------------------
                    // Consume exactly one frame.
                    //
                    // This preserves support for:
                    //
                    // [frame][frame][frame]
                    //
                    // in a single TCP read().
                    // --------------------------------------------------

                    buffer.erase(
                        buffer.begin(),
                        buffer.begin() +
                            static_cast<std::ptrdiff_t>(
                                total_frame_size));
                }
            }

            // ==================================================
            // PEER CLOSED
            // ==================================================

            else if (bytes_read == 0)
            {
                break;
            }

            // ==================================================
            // READ ERROR
            // ==================================================

            else
            {
                if (errno == EINTR)
                {
                    continue;
                }

                // SO_RCVTIMEO timeout.
                if (errno == EAGAIN ||
                    errno == EWOULDBLOCK)
                {
                    break;
                }

                std::cerr
                    << "[Warning] read() failed: "
                    << std::strerror(errno)
                    << '\n';

                break;
            }
        }

        close(client_socket);
    }

    std::cout
        << "[Worker "
        << worker_id
        << "] offline.\n";
}

// ==================================================
// MAIN
// ==================================================

int main()
{
    // ==================================================
    // SIGNAL SETUP
    // ==================================================

    struct sigaction sa{};

    sa.sa_handler = signal_handler;

    sigemptyset(&sa.sa_mask);

    // Do not use SA_RESTART.
    // We want SIGINT to interrupt accept().
    sa.sa_flags = 0;

    if (sigaction(
            SIGINT,
            &sa,
            nullptr) < 0)
    {
        std::cerr
            << "[Fatal error] sigaction() failed: "
            << std::strerror(errno)
            << '\n';

        return 1;
    }

    // ==================================================
    // CREATE SERVER SOCKET
    // ==================================================

    server_fd =
        socket(
            AF_INET,
            SOCK_STREAM,
            0);

    if (server_fd < 0)
    {
        std::cerr
            << "[Fatal error] socket() failed: "
            << std::strerror(errno)
            << '\n';

        return 1;
    }

    // ==================================================
    // SO_REUSEADDR
    // ==================================================

    int opt = 1;

    if (setsockopt(
            server_fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &opt,
            sizeof(opt)) < 0)
    {
        std::cerr
            << "[Fatal error] SO_REUSEADDR failed: "
            << std::strerror(errno)
            << '\n';

        close(server_fd);
        server_fd = -1;

        return 1;
    }

    // ==================================================
    // ADDRESS
    // ==================================================

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_addr.s_addr =
        htonl(INADDR_ANY);

    address.sin_port =
        htons(SERVER_PORT);

    // ==================================================
    // BIND
    // ==================================================

    if (bind(
            server_fd,
            reinterpret_cast<sockaddr *>(
                &address),
            sizeof(address)) < 0)
    {
        std::cerr
            << "[Fatal error] bind() failed: "
            << std::strerror(errno)
            << '\n';

        close(server_fd);
        server_fd = -1;

        return 1;
    }

    // ==================================================
    // LISTEN
    // ==================================================

    if (listen(
            server_fd,
            LISTEN_BACKLOG) < 0)
    {
        std::cerr
            << "[Fatal error] listen() failed: "
            << std::strerror(errno)
            << '\n';

        close(server_fd);
        server_fd = -1;

        return 1;
    }

    std::cout
        << "Project Ares V1 online... port: "
        << SERVER_PORT
        << '\n';

    std::cout
        << "Architecture: 16-way LRU cache. "
        << "capacity 160k keys.\n";

    // ==================================================
    // START WORKERS
    // ==================================================

    std::vector<std::thread> workers;

    try
    {
        for (int i = 0;
             i < NUM_WORKERS;
             ++i)
        {
            workers.emplace_back(
                worker_thread,
                i);
        }
    }
    catch (const std::exception &ex)
    {
        std::cerr
            << "[Fatal error] worker creation failed: "
            << ex.what()
            << '\n';

        keep_running = false;

        cv.notify_all();

        for (auto &worker : workers)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }

        while (!task_queue.empty())
        {
            close(task_queue.front());
            task_queue.pop();
        }

        close(server_fd);
        server_fd = -1;

        return 1;
    }

    // ==================================================
    // ACCEPT LOOP
    // ==================================================

    while (keep_running)
    {
        int client_socket =
            accept(
                server_fd,
                nullptr,
                nullptr);

        if (client_socket >= 0)
        {
            {
                std::lock_guard<std::mutex> lock(
                    queue_mutex);

                task_queue.push(
                    client_socket);
            }

            cv.notify_one();

            continue;
        }

        // SIGINT interrupted accept().
        if (errno == EINTR)
        {
            if (shutdown_requested)
            {
                break;
            }

            continue;
        }

        // Another error while shutting down.
        if (shutdown_requested)
        {
            break;
        }

        std::cerr
            << "[Warning] accept() failed: "
            << std::strerror(errno)
            << '\n';
    }

    // ==================================================
    // SHUTDOWN
    // ==================================================

    keep_running = false;

    cv.notify_all();

    std::cout
        << "\n[Server] sealing port "
        << SERVER_PORT
        << "...\n";

    if (server_fd >= 0)
    {
        close(server_fd);
        server_fd = -1;
    }

    // --------------------------------------------------
    // Workers finish their current client and exit.
    //
    // Workers blocked in read() can remain blocked until
    // SO_RCVTIMEO expires. This is a known V1 limitation.
    // V2 can improve shutdown latency separately.
    // --------------------------------------------------

    std::cout
        << "[Server] Awaiting thread pool convergence...\n";

    for (auto &worker : workers)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    // ==================================================
    // DRAIN QUEUED SOCKETS
    // ==================================================

    {
        std::lock_guard<std::mutex> lock(
            queue_mutex);

        while (!task_queue.empty())
        {
            close(task_queue.front());
            task_queue.pop();
        }
    }

    std::cout
        << "[Server] Engine offline. "
        << "All memory released safely.\n";

    return 0;
}