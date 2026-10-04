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
#include <utility>

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

constexpr std::size_t MAX_FRAME_SIZE = 1024 * 1024;

// Compact the stream buffer after enough bytes have
// been consumed rather than erasing after every frame.
constexpr std::size_t COMPACT_THRESHOLD = 4096;

// ==================================================
// SIGNAL / SHUTDOWN
// ==================================================

volatile std::sig_atomic_t shutdown_requested = 0;

std::atomic<bool> keep_running{true};

int server_fd = -1;

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
//
// V1 stored:
//
//     key
//     value
//
// while unordered_map also stored:
//
//     key
//
// V2 lets unordered_map own the key.
//
// key_ref points into the unordered_map key.
//
// It is valid while the corresponding map element exists.
// unordered_map rehash does not invalidate references/pointers
// to elements; erasing the element does.
// ==================================================

struct Node
{
    std::string value;

    const std::string *key_ref = nullptr;

    Node *prev = nullptr;
    Node *next = nullptr;

    explicit Node(std::string v)
        : value(std::move(v))
    {
    }
};

// ==================================================
// SHARD
// ==================================================

struct Shard
{
    std::size_t capacity = SHARD_CAPACITY;

    std::unordered_map<std::string, Node *> map;

    Node *head;
    Node *tail;

    std::mutex lock;

    Shard()
    {
        // Reserve capacity + 1 so insertion of the new
        // entry before eviction does not immediately force
        // a rehash.
        map.reserve(SHARD_CAPACITY + 1);

        head = new Node("");
        tail = new Node("");

        head->next = tail;
        tail->prev = head;
    }

    ~Shard()
    {
        for (auto &entry : map)
        {
            delete entry.second;
        }

        delete head;
        delete tail;
    }

    Shard(const Shard &) = delete;
    Shard &operator=(const Shard &) = delete;

    void removeNode(Node *node)
    {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void addHead(Node *node)
    {
        node->next = head->next;
        node->prev = head;

        head->next->prev = node;
        head->next = node;
    }

    void evictLRU()
    {
        Node *lru = tail->prev;

        if (lru == head)
        {
            return;
        }

        removeNode(lru);

        // key_ref remains valid until erase.
        map.erase(*lru->key_ref);

        delete lru;
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
// RESPONSE HELPERS
// ==================================================

bool send_all(
    int socket_fd,
    const char *data,
    std::size_t length)
{
    std::size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent =
            send(
                socket_fd,
                data + total_sent,
                length - total_sent,
                MSG_NOSIGNAL);

        if (sent > 0)
        {
            total_sent +=
                static_cast<std::size_t>(sent);

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

bool send_error(int fd)
{
    static constexpr char response[] = "ERR\n";

    return send_all(
        fd,
        response,
        sizeof(response) - 1);
}

// ==================================================
// PROCESS FRAME
// ==================================================

bool process_frame(
    int client_socket,
    const std::uint8_t *frame,
    std::size_t frame_size)
{
    if (frame_size < FRAME_HEADER_SIZE ||
        frame_size > MAX_FRAME_SIZE)
    {
        return send_error(client_socket);
    }

    const std::uint8_t opcode = frame[0];

    if (opcode != 0x01 && opcode != 0x02)
    {
        return send_error(client_socket);
    }

    std::uint32_t key_len_network = 0;
    std::uint32_t value_len_network = 0;

    std::memcpy(
        &key_len_network,
        frame + 1,
        sizeof(key_len_network));

    std::memcpy(
        &value_len_network,
        frame + 5,
        sizeof(value_len_network));

    const std::size_t key_len =
        ntohl(key_len_network);

    const std::size_t value_len =
        ntohl(value_len_network);

    // Overflow-safe validation.
    if (key_len >
        MAX_FRAME_SIZE - FRAME_HEADER_SIZE)
    {
        return send_error(client_socket);
    }

    if (value_len >
        MAX_FRAME_SIZE -
            FRAME_HEADER_SIZE -
            key_len)
    {
        return send_error(client_socket);
    }

    const std::size_t expected_frame_size =
        FRAME_HEADER_SIZE +
        key_len +
        value_len;

    if (expected_frame_size != frame_size)
    {
        return send_error(client_socket);
    }

    try
    {
        std::string key(
            reinterpret_cast<const char *>(
                frame + FRAME_HEADER_SIZE),
            key_len);

        const std::size_t shard_index =
            std::hash<std::string>{}(key) %
            NUM_SHARDS;

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
                        frame +
                        FRAME_HEADER_SIZE +
                        key_len),
                    value_len);

                if (it != shard.map.end())
                {
                    Node *node = it->second;

                    node->value =
                        std::move(value);

                    shard.removeNode(node);
                    shard.addHead(node);
                }
                else
                {
                    Node *node =
                        new Node(
                            std::move(value));

                    try
                    {
                        auto result =
                            shard.map.emplace(
                                std::move(key),
                                node);

                        if (!result.second)
                        {
                            delete node;

                            return send_error(
                                client_socket);
                        }

                        auto map_it =
                            result.first;

                        node->key_ref =
                            &map_it->first;

                        shard.addHead(node);

                        // New entry may temporarily make
                        // the shard contain capacity + 1.
                        if (shard.map.size() >
                            shard.capacity)
                        {
                            shard.evictLRU();
                        }
                    }
                    catch (...)
                    {
                        delete node;
                        throw;
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

                    // GET mutates LRU order.
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

        // Network I/O remains outside shard lock.
        return send_all(
            client_socket,
            response,
            response_len);
    }
    catch (const std::bad_alloc &)
    {
        // Keep worker alive under memory pressure.
        return send_error(client_socket);
    }
    catch (...)
    {
        return send_error(client_socket);
    }
}

// ==================================================
// WORKER
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

            if (!keep_running)
            {
                break;
            }

            client_socket =
                task_queue.front();

            task_queue.pop();
        }

        // ==================================================
        // SOCKET CONFIGURATION
        // ==================================================

        struct timeval tv{};

        tv.tv_sec =
            RECEIVE_TIMEOUT_SECONDS;

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

        // ==================================================
        // STREAM BUFFER
        // ==================================================

        std::vector<std::uint8_t> buffer;

        buffer.reserve(4096);

        std::size_t read_offset = 0;

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

                // Compact only when needed.
                if (read_offset > 0 &&
                    (buffer.size() + bytes >
                         MAX_FRAME_SIZE ||
                     read_offset >=
                         COMPACT_THRESHOLD))
                {
                    const std::size_t remaining =
                        buffer.size() -
                        read_offset;

                    std::memmove(
                        buffer.data(),
                        buffer.data() + read_offset,
                        remaining);

                    buffer.resize(remaining);

                    read_offset = 0;
                }

                // Ensure the buffered unread data cannot
                // exceed the maximum legal frame size.
                if (buffer.size() >
                        MAX_FRAME_SIZE ||
                    bytes >
                        MAX_FRAME_SIZE -
                            buffer.size())
                {
                    std::cerr
                        << "[Warning] client exceeded "
                        << "maximum frame size\n";

                    client_ok = false;
                    break;
                }

                buffer.insert(
                    buffer.end(),
                    temp_buffer,
                    temp_buffer + bytes);

                // ==================================================
                // PARSE FRAMES
                // ==================================================

                while (true)
                {
                    const std::size_t available =
                        buffer.size() -
                        read_offset;

                    if (available <
                        FRAME_HEADER_SIZE)
                    {
                        break;
                    }

                    const std::uint8_t *frame =
                        buffer.data() +
                        read_offset;

                    std::uint32_t key_len_network = 0;
                    std::uint32_t value_len_network = 0;

                    std::memcpy(
                        &key_len_network,
                        frame + 1,
                        sizeof(key_len_network));

                    std::memcpy(
                        &value_len_network,
                        frame + 5,
                        sizeof(value_len_network));

                    const std::size_t key_len =
                        ntohl(key_len_network);

                    const std::size_t value_len =
                        ntohl(value_len_network);

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

                    const std::size_t frame_size =
                        FRAME_HEADER_SIZE +
                        key_len +
                        value_len;

                    if (available < frame_size)
                    {
                        break;
                    }

                    if (!process_frame(
                            client_socket,
                            frame,
                            frame_size))
                    {
                        client_ok = false;
                        break;
                    }

                    read_offset += frame_size;

                    // All buffered data consumed.
                    if (read_offset ==
                        buffer.size())
                    {
                        buffer.clear();
                        read_offset = 0;
                        break;
                    }

                    // Avoid retaining a large consumed prefix.
                    if (read_offset >=
                            COMPACT_THRESHOLD &&
                        read_offset * 2 >=
                            buffer.size())
                    {
                        const std::size_t remaining =
                            buffer.size() -
                            read_offset;

                        std::memmove(
                            buffer.data(),
                            buffer.data() +
                                read_offset,
                            remaining);

                        buffer.resize(remaining);

                        read_offset = 0;
                    }
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
            // ERROR
            // ==================================================

            else
            {
                if (errno == EINTR)
                {
                    continue;
                }

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
    struct sigaction sa{};

    sa.sa_handler = signal_handler;

    sigemptyset(&sa.sa_mask);

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

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_addr.s_addr =
        htonl(INADDR_ANY);

    address.sin_port =
        htons(SERVER_PORT);

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
        << "Project Ares V2 online... port: "
        << SERVER_PORT
        << '\n';

    std::cout
        << "Architecture: 16-way LRU cache. "
        << "capacity 160k keys.\n";

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
    // ACCEPT
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
            if (!keep_running)
            {
                close(client_socket);
                break;
            }

            {
                std::lock_guard<std::mutex> lock(
                    queue_mutex);

                task_queue.push(
                    client_socket);
            }

            cv.notify_one();

            continue;
        }

        if (errno == EINTR)
        {
            if (shutdown_requested)
            {
                break;
            }

            continue;
        }

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

    std::cout
        << "[Server] Awaiting thread pool convergence...\n";

    for (auto &worker : workers)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

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