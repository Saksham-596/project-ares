#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#elif defined(__APPLE__)
#include <sys/event.h>
#include <sys/time.h>
#else
#error "Project Ares V3 supports Linux and macOS only."
#endif

// ============================================================
// PROJECT ARES V3
//
// Performance design:
//   - one acceptor
//   - stable per-worker event loop (epoll/kqueue)
//   - persistent worker-owned connections
//   - no pollfd rebuild per iteration
//   - wake pipe used only when a new connection is assigned
//   - bounded input/output buffers
//   - unordered_map<Node*> + intrusive LRU
//   - network I/O never occurs while a shard is locked
// ============================================================

constexpr std::size_t NUM_SHARDS = 16;
constexpr std::size_t SHARD_CAPACITY = 10000;

constexpr int NUM_WORKERS = 4;
constexpr int SERVER_PORT = 8080;
constexpr int LISTEN_BACKLOG = 1000;

constexpr std::size_t FRAME_HEADER_SIZE = 9;
constexpr std::size_t MAX_FRAME_SIZE = 1024 * 1024;

constexpr std::size_t INPUT_RESERVE = 8192;
constexpr std::size_t MAX_OUTPUT_BUFFER = 64 * 1024;

constexpr std::size_t READ_BUFFER_SIZE = 16 * 1024;
constexpr std::size_t COMPACT_THRESHOLD = 4096;

// ============================================================
// SHUTDOWN
// ============================================================

volatile std::sig_atomic_t shutdown_requested = 0;

std::atomic<bool> keep_running{true};

void signal_handler(int)
{
    // Only touch sig_atomic_t state from the signal handler.
    // The main thread performs the ordinary atomic shutdown transition.
    shutdown_requested = 1;
}

// ============================================================
// RESPONSE
// ============================================================

enum class ResponseCode : std::uint8_t
{
    OK,
    NIL,
    ERR
};

inline const char *response_data(ResponseCode code) noexcept
{
    switch (code)
    {
    case ResponseCode::OK:
        return "OK\n";

    case ResponseCode::NIL:
        return "nil\n";

    default:
        return "ERR\n";
    }
}

inline std::size_t response_size(ResponseCode code) noexcept
{
    switch (code)
    {
    case ResponseCode::OK:
        return 3;

    case ResponseCode::NIL:
        return 4;

    default:
        return 4;
    }
}

// ============================================================
// HASH
// ============================================================

struct StringHash
{
    using is_transparent = void;

    std::size_t operator()(const std::string &value) const noexcept
    {
        return std::hash<std::string>{}(value);
    }

    std::size_t operator()(std::string_view value) const noexcept
    {
        return std::hash<std::string_view>{}(value);
    }
};

// ============================================================
// LRU NODE
// ============================================================

struct Node
{
    std::string value;

    // Points to the key stored inside Shard::map.
    // References/pointers to unordered_map elements remain valid across
    // rehash; the pointed-to element is removed only during eviction.
    const std::string *key_ref = nullptr;

    Node *prev = nullptr;
    Node *next = nullptr;

    explicit Node(std::string value_in)
        : value(std::move(value_in))
    {
    }
};

// ============================================================
// SHARD
// ============================================================

struct Shard
{
    using Map =
        std::unordered_map<
            std::string,
            Node *,
            StringHash>;

    std::size_t capacity =
        SHARD_CAPACITY;

    Map map;

    Node *head = nullptr;
    Node *tail = nullptr;

    std::mutex lock;

    Shard()
    {
        map.reserve(
            SHARD_CAPACITY + 1);

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

    inline void remove_node(
        Node *node) noexcept
    {
        node->prev->next =
            node->next;

        node->next->prev =
            node->prev;
    }

    inline void add_head(
        Node *node) noexcept
    {
        node->next =
            head->next;

        node->prev =
            head;

        head->next->prev =
            node;

        head->next =
            node;
    }

    inline void touch(
        Node *node) noexcept
    {
        if (head->next == node)
        {
            return;
        }

        remove_node(node);
        add_head(node);
    }

    inline void evict_lru() noexcept
    {
        Node *lru =
            tail->prev;

        if (lru == head)
        {
            return;
        }

        remove_node(lru);

        map.erase(
            *lru->key_ref);

        delete lru;
    }
};

// ============================================================
// DATABASE
// ============================================================

std::vector<Shard>
    database_shard(NUM_SHARDS);

// ============================================================
// PARSED FRAME
// ============================================================

struct ParsedFrame
{
    std::uint8_t opcode = 0;

    std::string_view key;

    std::string_view value;
};

// ============================================================
// CACHE HOT PATH
// ============================================================

ResponseCode process_frame(
    const ParsedFrame &frame) noexcept
{
    if (frame.opcode != 0x01 &&
        frame.opcode != 0x02)
    {
        return ResponseCode::ERR;
    }

    try
    {
        std::string key(
            frame.key.data(),
            frame.key.size());

        const std::size_t hash =
            std::hash<std::string>{}(key);

        Shard &shard =
            database_shard[
                hash &
                (NUM_SHARDS - 1)];

        std::string value;

        // Allocate/copy SET value before taking the shard lock.
        if (frame.opcode == 0x01)
        {
            value.assign(
                frame.value.data(),
                frame.value.size());
        }

        std::lock_guard<std::mutex>
            guard(shard.lock);

        auto it =
            shard.map.find(key);

        // ====================================================
        // SET
        // ====================================================

        if (frame.opcode == 0x01)
        {
            if (it != shard.map.end())
            {
                Node *node =
                    it->second;

                node->value =
                    std::move(value);

                shard.touch(node);

                return ResponseCode::OK;
            }

            Node *node = nullptr;

            try
            {
                node =
                    new Node(
                        std::move(value));

                auto result =
                    shard.map.emplace(
                        std::move(key),
                        node);

                if (!result.second)
                {
                    delete node;
                    return ResponseCode::ERR;
                }

                node->key_ref =
                    &result.first->first;

                shard.add_head(node);

                if (shard.map.size() >
                    shard.capacity)
                {
                    shard.evict_lru();
                }

                return ResponseCode::OK;
            }
            catch (...)
            {
                delete node;
                throw;
            }
        }

        // ====================================================
        // GET
        // ====================================================

        if (it == shard.map.end())
        {
            return ResponseCode::NIL;
        }

        shard.touch(
            it->second);

        return ResponseCode::OK;
    }
    catch (const std::bad_alloc &)
    {
        return ResponseCode::ERR;
    }
    catch (...)
    {
        return ResponseCode::ERR;
    }
}

// ============================================================
// NONBLOCKING
// ============================================================

bool set_nonblocking(
    int fd)
{
    const int flags =
        fcntl(
            fd,
            F_GETFL,
            0);

    if (flags < 0)
    {
        return false;
    }

    return fcntl(
               fd,
               F_SETFL,
               flags | O_NONBLOCK) == 0;
}

// ============================================================
// CLIENT SESSION
// ============================================================

struct ClientSession
{
    int fd = -1;

    std::vector<std::uint8_t>
        input;

    std::size_t input_size = 0;

    std::size_t read_offset = 0;

    std::unique_ptr<char[]>
        output;

    std::size_t output_size = 0;

    std::size_t write_offset = 0;

    bool peer_closed = false;

    bool close_after_flush = false;

    bool active = true;

    // Worker-thread-only state used to merge multiple kernel events
    // for one session into a single processing pass.
    std::uint64_t event_batch = 0;

    std::uint8_t event_mask = 0;

    explicit ClientSession(
        int socket_fd)
        : fd(socket_fd)
    {
    }

    ClientSession(
        const ClientSession &) = delete;

    ClientSession &operator=(
        const ClientSession &) = delete;

    ~ClientSession()
    {
        if (fd >= 0)
        {
            close(fd);
        }
    }

    inline std::size_t pending_input()
        const noexcept
    {
        return input_size -
               read_offset;
    }

    inline std::size_t pending_output()
        const noexcept
    {
        return output_size -
               write_offset;
    }
};

// ============================================================
// WORKER CONTEXT
// ============================================================

struct WorkerContext
{
    std::mutex queue_mutex;

    std::queue<int>
        pending_clients;

    int wake_read = -1;

    int wake_write = -1;
};

// ============================================================
// WORKER WAKE
// ============================================================

void wake_worker(
    WorkerContext &worker)
{
    const char byte = 'x';

    while (true)
    {
        const ssize_t result =
            write(
                worker.wake_write,
                &byte,
                1);

        if (result == 1)
        {
            return;
        }

        if (result < 0 &&
            errno == EINTR)
        {
            continue;
        }

        if (result < 0 &&
            (errno == EAGAIN ||
             errno == EWOULDBLOCK))
        {
            // One byte already in the pipe is sufficient to wake the
            // worker. The queue itself is the authoritative state.
            return;
        }

        return;
    }
}

// ============================================================
// DRAIN WAKE PIPE
// ============================================================

void drain_wake_pipe(
    int fd)
{
    char buffer[256];

    while (true)
    {
        const ssize_t n =
            read(
                fd,
                buffer,
                sizeof(buffer));

        if (n > 0)
        {
            continue;
        }

        if (n < 0 &&
            errno == EINTR)
        {
            continue;
        }

        break;
    }
}

// ============================================================
// IMPORT CLIENTS
// ============================================================

void import_clients(
    WorkerContext &worker,
    std::unordered_map<
        int,
        std::unique_ptr<ClientSession>> &sessions)
{
    drain_wake_pipe(
        worker.wake_read);

    while (true)
    {
        int fd = -1;

        {
            std::lock_guard<std::mutex>
                guard(worker.queue_mutex);

            if (worker.pending_clients.empty())
            {
                break;
            }

            fd =
                worker.pending_clients.front();

            worker.pending_clients.pop();
        }

        if (!keep_running)
        {
            close(fd);
            continue;
        }

        if (!set_nonblocking(fd))
        {
            close(fd);
            continue;
        }

        std::unique_ptr<ClientSession>
            client;

        try
        {
            client =
                std::make_unique<
                    ClientSession>(fd);

            client->input.reserve(
                INPUT_RESERVE);

            client->input.resize(
                INPUT_RESERVE);

            client->output =
                std::make_unique<char[]>(
                    MAX_OUTPUT_BUFFER);

            // Do not move ownership into the map until the map insertion
            // itself succeeds. If unordered_map allocation throws, the
            // local unique_ptr still owns and closes the socket.
            auto result =
                sessions.emplace(
                    fd,
                    nullptr);

            if (!result.second)
            {
                // A duplicate live fd cannot normally happen because an
                // open descriptor cannot simultaneously be reused by accept.
                // The local RAII object closes this rejected descriptor.
                continue;
            }

            result.first->second =
                std::move(client);
        }
        catch (...)
        {
            // If client owns fd, its destructor closes it.
            // If construction itself failed before ownership existed,
            // close the raw descriptor here.
            if (!client)
            {
                close(fd);
            }
        }
    }
}

// ============================================================
// INPUT COMPACTION
// ============================================================

inline void compact_input(
    ClientSession &client) noexcept
{
    if (client.read_offset == 0)
    {
        return;
    }

    const std::size_t remaining =
        client.input_size -
        client.read_offset;

    if (remaining != 0)
    {
        std::memmove(
            client.input.data(),
            client.input.data() +
                client.read_offset,
            remaining);
    }

    client.input_size =
        remaining;

    client.read_offset = 0;
}

// ============================================================
// ENSURE INPUT SPACE
// ============================================================

bool ensure_input_space(
    ClientSession &client,
    std::size_t needed)
{
    if (client.input.size() -
            client.input_size >=
        needed)
    {
        return true;
    }

    if (client.read_offset != 0)
    {
        compact_input(client);

        if (client.input.size() -
                client.input_size >=
            needed)
        {
            return true;
        }
    }

    if (client.input_size >=
        MAX_FRAME_SIZE)
    {
        return false;
    }

    const std::size_t required =
        client.input_size +
        needed;

    std::size_t target =
        client.input.empty()
            ? INPUT_RESERVE
            : client.input.size();

    while (target < required)
    {
        if (target >=
            MAX_FRAME_SIZE / 2)
        {
            target =
                MAX_FRAME_SIZE;

            break;
        }

        target *= 2;
    }

    if (target >
        MAX_FRAME_SIZE)
    {
        target =
            MAX_FRAME_SIZE;
    }

    if (target < required)
    {
        return false;
    }

    client.input.resize(target);

    return true;
}

// ============================================================
// OUTPUT COMPACTION
// ============================================================

inline void compact_output(
    ClientSession &client) noexcept
{
    if (client.write_offset == 0)
    {
        return;
    }

    const std::size_t remaining =
        client.output_size -
        client.write_offset;

    if (remaining != 0)
    {
        std::memmove(
            client.output.get(),
            client.output.get() +
                client.write_offset,
            remaining);
    }

    client.output_size =
        remaining;

    client.write_offset = 0;
}

// ============================================================
// APPEND RESPONSE
// ============================================================

inline bool append_response(
    ClientSession &client,
    ResponseCode code) noexcept
{
    const std::size_t len =
        response_size(code);

    if (client.write_offset != 0 &&
        client.output_size + len >
            MAX_OUTPUT_BUFFER)
    {
        compact_output(client);
    }

    if (client.output_size + len >
        MAX_OUTPUT_BUFFER)
    {
        return false;
    }

    std::memcpy(
        client.output.get() +
            client.output_size,
        response_data(code),
        len);

    client.output_size +=
        len;

    return true;
}

// ============================================================
// PROCESS INPUT BUFFER
// ============================================================

bool process_input_buffer(
    ClientSession &client)
{
    while (true)
    {
        const std::size_t available =
            client.pending_input();

        if (available <
            FRAME_HEADER_SIZE)
        {
            break;
        }

        const std::uint8_t *frame =
            client.input.data() +
            client.read_offset;

        std::uint32_t key_net = 0;
        std::uint32_t value_net = 0;

        std::memcpy(
            &key_net,
            frame + 1,
            sizeof(key_net));

        std::memcpy(
            &value_net,
            frame + 5,
            sizeof(value_net));

        const std::size_t key_len =
            ntohl(key_net);

        const std::size_t value_len =
            ntohl(value_net);

        if (key_len >
                MAX_FRAME_SIZE -
                    FRAME_HEADER_SIZE ||
            value_len >
                MAX_FRAME_SIZE -
                    FRAME_HEADER_SIZE -
                    key_len)
        {
            // Framing is unrecoverable. Queue at most one ERR and discard
            // the remaining buffered bytes because the connection closes.
            // Clearing the input also prevents repeatedly parsing the same
            // malformed header on later writable/readable events.
            (void)append_response(
                client,
                ResponseCode::ERR);

            client.input_size = 0;
            client.read_offset = 0;

            client.close_after_flush =
                true;

            return true;
        }

        const std::size_t frame_size =
            FRAME_HEADER_SIZE +
            key_len +
            value_len;

        if (available < frame_size)
        {
            break;
        }

        // Each response is at most 4 bytes. Stop consuming complete
        // requests while the bounded output queue cannot accept another
        // response.
        if (client.pending_output() + 4 >
            MAX_OUTPUT_BUFFER)
        {
            break;
        }

        ParsedFrame parsed;

        parsed.opcode =
            frame[0];

        parsed.key =
            std::string_view(
                reinterpret_cast<
                    const char *>(
                    frame +
                    FRAME_HEADER_SIZE),
                key_len);

        parsed.value =
            std::string_view(
                reinterpret_cast<
                    const char *>(
                    frame +
                    FRAME_HEADER_SIZE +
                    key_len),
                value_len);

        const ResponseCode response =
            process_frame(parsed);

        if (!append_response(
                client,
                response))
        {
            client.close_after_flush =
                true;

            return true;
        }

        client.read_offset +=
            frame_size;

        if (client.read_offset ==
            client.input_size)
        {
            client.input_size = 0;
            client.read_offset = 0;

            break;
        }

        if (client.read_offset >=
                COMPACT_THRESHOLD &&
            client.read_offset * 2 >=
                client.input_size)
        {
            compact_input(client);
        }
    }

    return true;
}

// ============================================================
// READ CLIENT
// ============================================================

bool read_client(
    ClientSession &client,
    bool eof_hint = false)
{
    // kqueue EV_EOF means the peer performed an orderly shutdown.
    // It does NOT mean that unread bytes have disappeared from the
    // receive queue. Mark the peer closed, then drain/process data.
    if (eof_hint)
    {
        client.peer_closed = true;
    }

    if (!process_input_buffer(client))
    {
        return false;
    }

    if (client.close_after_flush)
    {
        return true;
    }

    while (true)
    {
        // If output is saturated, stop reading. Already-buffered complete
        // frames will be processed after output drains.
        if (client.pending_output() + 4 >
            MAX_OUTPUT_BUFFER)
        {
            return true;
        }

        if (!ensure_input_space(
                client,
                1))
        {
            return false;
        }

        std::size_t free_space =
            client.input.size() -
            client.input_size;

        if (free_space == 0)
        {
            return false;
        }

        if (free_space >
            READ_BUFFER_SIZE)
        {
            free_space =
                READ_BUFFER_SIZE;
        }

        const ssize_t n =
            read(
                client.fd,
                client.input.data() +
                    client.input_size,
                free_space);

        if (n > 0)
        {
            client.input_size +=
                static_cast<std::size_t>(
                    n);

            if (!process_input_buffer(
                    client))
            {
                return false;
            }

            if (client.close_after_flush)
            {
                return true;
            }

            continue;
        }

        if (n == 0)
        {
            client.peer_closed =
                true;

            // Process any final complete frame. A partial frame is simply
            // discarded when the half-closed connection is retired.
            if (!process_input_buffer(
                    client))
            {
                return false;
            }

            return true;
        }

        if (errno == EINTR)
        {
            continue;
        }

        if (errno == EAGAIN ||
            errno == EWOULDBLOCK)
        {
            return true;
        }

        return false;
    }
}

// ============================================================
// WRITE CLIENT
// ============================================================

bool write_client(
    ClientSession &client)
{
    while (
        client.write_offset <
        client.output_size)
    {
        const ssize_t n =
            send(
                client.fd,
                client.output.get() +
                    client.write_offset,
                client.output_size -
                    client.write_offset,
#ifdef MSG_NOSIGNAL
                MSG_NOSIGNAL
#else
                0
#endif
            );

        if (n > 0)
        {
            client.write_offset +=
                static_cast<std::size_t>(
                    n);

            continue;
        }

        if (n < 0 &&
            errno == EINTR)
        {
            continue;
        }

        if (n < 0 &&
            (errno == EAGAIN ||
             errno == EWOULDBLOCK))
        {
            return true;
        }

        return false;
    }

    client.output_size = 0;
    client.write_offset = 0;

    return true;
}

// ============================================================
// EVENT BACKEND
// ============================================================

#if defined(__linux__)

struct EventLoop
{
    int fd = -1;

    bool init()
    {
        fd =
            epoll_create1(
                EPOLL_CLOEXEC);

        return fd >= 0;
    }

    bool add_fd(
        int target,
        void *ptr,
        bool writable)
    {
        epoll_event ev{};

        ev.events =
            EPOLLIN |
            EPOLLRDHUP;

        if (writable)
        {
            ev.events |=
                EPOLLOUT;
        }

        ev.data.ptr =
            ptr;

        return epoll_ctl(
                   fd,
                   EPOLL_CTL_ADD,
                   target,
                   &ev) == 0;
    }

    bool mod_fd(
        int target,
        void *ptr,
        bool readable,
        bool writable)
    {
        epoll_event ev{};

        ev.events =
            readable
                ? (EPOLLIN | EPOLLRDHUP)
                : 0;

        if (writable)
        {
            ev.events |=
                EPOLLOUT;
        }

        ev.data.ptr =
            ptr;

        return epoll_ctl(
                   fd,
                   EPOLL_CTL_MOD,
                   target,
                   &ev) == 0;
    }

    bool del_fd(
        int target) noexcept
    {
        while (true)
        {
            if (epoll_ctl(
                    fd,
                    EPOLL_CTL_DEL,
                    target,
                    nullptr) == 0)
            {
                return true;
            }

            if (errno == EINTR)
            {
                continue;
            }

            return errno == ENOENT ||
                   errno == EBADF;
        }
    }

    int wait(
        epoll_event *events,
        int max_events)
    {
        return epoll_wait(
            fd,
            events,
            max_events,
            -1);
    }

    void close_loop() noexcept
    {
        if (fd >= 0)
        {
            close(fd);
            fd = -1;
        }
    }
};

#elif defined(__APPLE__)

struct EventLoop
{
    int fd = -1;

    bool init()
    {
        fd = kqueue();

        return fd >= 0;
    }

    // Register both filters in one changelist. If the change fails,
    // remove both filters defensively so the caller never retains a
    // half-registered session.
    bool add_fd(
        int target,
        void *ptr,
        bool writable)
    {
        struct kevent changes[2];

        int count = 1;

        EV_SET(
            &changes[0],
            target,
            EVFILT_READ,
            EV_ADD | EV_ENABLE,
            0,
            0,
            ptr);

        if (writable)
        {
            EV_SET(
                &changes[1],
                target,
                EVFILT_WRITE,
                EV_ADD | EV_ENABLE,
                0,
                0,
                ptr);

            count = 2;
        }

        // With no event output list, successful kevent() returns 0.
        if (kevent(
                fd,
                changes,
                count,
                nullptr,
                0,
                nullptr) == 0)
        {
            return true;
        }

        del_fd(target);

        return false;
    }

    bool set_write(
        int target,
        void *ptr,
        bool writable)
    {
        if (writable)
        {
            struct kevent change;

            EV_SET(
                &change,
                target,
                EVFILT_WRITE,
                EV_ADD | EV_ENABLE,
                0,
                0,
                ptr);

            while (true)
            {
                if (kevent(
                        fd,
                        &change,
                        1,
                        nullptr,
                        0,
                        nullptr) == 0)
                {
                    return true;
                }

                if (errno == EINTR)
                {
                    continue;
                }

                return false;
            }
        }

        return del_write(target);
    }

    bool del_write(
        int target) noexcept
    {
        struct kevent change;

        EV_SET(
            &change,
            target,
            EVFILT_WRITE,
            EV_DELETE,
            0,
            0,
            nullptr);

        while (true)
        {
            if (kevent(
                    fd,
                    &change,
                    1,
                    nullptr,
                    0,
                    nullptr) == 0)
            {
                return true;
            }

            if (errno == EINTR)
            {
                continue;
            }

            return errno == ENOENT ||
                   errno == EBADF;
        }
    }

    bool mod_fd(
        int target,
        void *ptr,
        bool readable,
        bool writable)
    {
        // READ is normally persistent. Once FIN or close-after-flush is
        // known, disable READ too. Otherwise kqueue can repeatedly report
        // persistent EOF while the worker is waiting to flush output.
        if (!readable)
        {
            struct kevent change;

            EV_SET(
                &change,
                target,
                EVFILT_READ,
                EV_DELETE,
                0,
                0,
                nullptr);

            while (true)
            {
                if (kevent(
                        fd,
                        &change,
                        1,
                        nullptr,
                        0,
                        nullptr) == 0)
                {
                    break;
                }

                if (errno == EINTR)
                {
                    continue;
                }

                if (errno != ENOENT &&
                    errno != EBADF)
                {
                    return false;
                }

                break;
            }
        }
        else
        {
            struct kevent change;

            EV_SET(
                &change,
                target,
                EVFILT_READ,
                EV_ADD | EV_ENABLE,
                0,
                0,
                ptr);

            while (true)
            {
                if (kevent(
                        fd,
                        &change,
                        1,
                        nullptr,
                        0,
                        nullptr) == 0)
                {
                    break;
                }

                if (errno == EINTR)
                {
                    continue;
                }

                return false;
            }
        }

        return set_write(
            target,
            ptr,
            writable);
    }

    bool del_fd(
        int target) noexcept
    {
        struct kevent changes[2];

        EV_SET(
            &changes[0],
            target,
            EVFILT_READ,
            EV_DELETE,
            0,
            0,
            nullptr);

        EV_SET(
            &changes[1],
            target,
            EVFILT_WRITE,
            EV_DELETE,
            0,
            0,
            nullptr);

        bool read_ok = false;
        bool write_ok = false;

        // Delete independently so failure/ENOENT for one filter cannot
        // hide the state of the other filter.
        while (true)
        {
            if (kevent(
                    fd,
                    &changes[0],
                    1,
                    nullptr,
                    0,
                    nullptr) == 0)
            {
                read_ok = true;
                break;
            }

            if (errno == EINTR)
            {
                continue;
            }

            read_ok =
                errno == ENOENT ||
                errno == EBADF;

            break;
        }

        while (true)
        {
            if (kevent(
                    fd,
                    &changes[1],
                    1,
                    nullptr,
                    0,
                    nullptr) == 0)
            {
                write_ok = true;
                break;
            }

            if (errno == EINTR)
            {
                continue;
            }

            write_ok =
                errno == ENOENT ||
                errno == EBADF;

            break;
        }

        return read_ok &&
               write_ok;
    }

    int wait(
        struct kevent *events,
        int max_events)
    {
        return kevent(
            fd,
            nullptr,
            0,
            events,
            max_events,
            nullptr);
    }

    void close_loop() noexcept
    {
        if (fd >= 0)
        {
            close(fd);
            fd = -1;
        }
    }
};

#endif

// ============================================================
// WORKER EVENT MASKS
// ============================================================

enum : std::uint8_t
{
    EVENT_READ =
        1u << 0,

    EVENT_WRITE =
        1u << 1,

    EVENT_ERROR =
        1u << 2,

    EVENT_EOF =
        1u << 3
};

// ============================================================
// WORKER THREAD
// ============================================================

void worker_thread(
    WorkerContext &worker,
    int worker_id)
{
    std::cout
        << "[Worker "
        << worker_id
        << "] online.\n";

    EventLoop loop;

    auto close_pending_clients =
        [&]()
    {
        std::lock_guard<std::mutex>
            guard(worker.queue_mutex);

        while (!worker.pending_clients.empty())
        {
            close(
                worker.pending_clients.front());

            worker.pending_clients.pop();
        }
    };

    if (!loop.init())
    {
        std::cerr
            << "[Worker "
            << worker_id
            << "] event backend initialization failed.\n";

        close_pending_clients();

        return;
    }

    // The map owns every session. Kernel event data points into these
    // heap-stable ClientSession objects.
    std::unordered_map<
        int,
        std::unique_ptr<ClientSession>>
        sessions;

    sessions.reserve(64);

    std::unordered_map<
        int,
        bool>
        registered;

    registered.reserve(64);

    struct WakeTag
    {
    };

    WakeTag wake_tag;

    if (!loop.add_fd(
            worker.wake_read,
            &wake_tag,
            false))
    {
        std::cerr
            << "[Worker "
            << worker_id
            << "] wake pipe registration failed.\n";

        loop.close_loop();

        close_pending_clients();

        return;
    }

#if defined(__linux__)

    std::vector<
        epoll_event>
        events(64);

#else

    std::vector<
        struct kevent>
        events(64);

#endif

    auto register_pending =
        [&]()
    {
        import_clients(
            worker,
            sessions);

        std::vector<int>
            failed;

        failed.reserve(4);

        for (auto &entry :
             sessions)
        {
            const int fd =
                entry.first;

            ClientSession *client =
                entry.second.get();

            if (!client->active ||
                registered.find(fd) !=
                    registered.end())
            {
                continue;
            }

            if (!loop.add_fd(
                    fd,
                    client,
                    client->pending_output() != 0))
            {
                client->active = false;

                failed.push_back(fd);

                continue;
            }

            try
            {
                registered.emplace(
                    fd,
                    true);
            }
            catch (...)
            {
                loop.del_fd(fd);

                client->active = false;

                failed.push_back(fd);
            }
        }

        for (int fd :
             failed)
        {
            registered.erase(fd);

            sessions.erase(fd);
        }
    };

    auto retire =
        [&](ClientSession *client)
    {
        if (!client->active)
        {
            return;
        }

        const int fd =
            client->fd;

        (void)loop.del_fd(fd);

        registered.erase(fd);

        client->active = false;
    };

    std::uint64_t batch_id = 0;

    while (keep_running)
    {
        register_pending();

        if (!keep_running)
        {
            break;
        }

        const int ready =
            loop.wait(
                events.data(),
                static_cast<int>(
                    events.size()));

        if (ready < 0)
        {
            if (errno == EINTR)
            {
                if (shutdown_requested)
                {
                    break;
                }

                continue;
            }

            break;
        }

        ++batch_id;

        if (batch_id == 0)
        {
            batch_id = 1;

            for (auto &entry :
                 sessions)
            {
                entry.second->event_batch =
                    0;
            }
        }

        std::vector<
            ClientSession *>
            ready_clients;

        ready_clients.reserve(
            static_cast<std::size_t>(
                ready));

        // ====================================================
        // CLASSIFY ENTIRE KERNEL BATCH FIRST
        //
        // kqueue may return READ and WRITE as two separate events
        // for one fd. We merge them before touching the session.
        // ====================================================

#if defined(__linux__)

        for (int i = 0;
             i < ready;
             ++i)
        {
            epoll_event &event =
                events[i];

            if (event.data.ptr ==
                &wake_tag)
            {
                continue;
            }

            auto *client =
                static_cast<
                    ClientSession *>(
                    event.data.ptr);

            if (!client->active)
            {
                continue;
            }

            if (client->event_batch !=
                batch_id)
            {
                client->event_batch =
                    batch_id;

                client->event_mask = 0;

                ready_clients.push_back(
                    client);
            }

            if (event.events &
                EPOLLIN)
            {
                client->event_mask |=
                    EVENT_READ;
            }

            if (event.events &
                EPOLLOUT)
            {
                client->event_mask |=
                    EVENT_WRITE;
            }

            if (event.events &
                EPOLLERR)
            {
                client->event_mask |=
                    EVENT_ERROR;
            }

            if (event.events &
                (EPOLLRDHUP |
                 EPOLLHUP))
            {
                client->event_mask |=
                    EVENT_EOF;
            }
        }

#else

        for (int i = 0;
             i < ready;
             ++i)
        {
            struct kevent &event =
                events[i];

            if (event.udata ==
                &wake_tag)
            {
                continue;
            }

            auto *client =
                static_cast<
                    ClientSession *>(
                    event.udata);

            if (!client->active)
            {
                continue;
            }

            if (client->event_batch !=
                batch_id)
            {
                client->event_batch =
                    batch_id;

                client->event_mask = 0;

                ready_clients.push_back(
                    client);
            }

            if (event.flags &
                EV_ERROR)
            {
                client->event_mask |=
                    EVENT_ERROR;
            }

            if (event.flags &
                EV_EOF)
            {
                client->event_mask |=
                    EVENT_EOF;
            }

            if (event.filter ==
                EVFILT_READ)
            {
                client->event_mask |=
                    EVENT_READ;
            }
            else if (event.filter ==
                     EVFILT_WRITE)
            {
                client->event_mask |=
                    EVENT_WRITE;
            }
        }

#endif

        // ====================================================
        // PROCESS EACH SESSION ONCE
        // ====================================================

        std::vector<int>
            retired_fds;

        retired_fds.reserve(
            ready_clients.size());

        for (ClientSession *client :
             ready_clients)
        {
            if (!client->active)
            {
                continue;
            }

            const std::uint8_t mask =
                client->event_mask;

            bool alive = true;

            // Kernel/filter errors are handled before normal I/O.
            if (mask & EVENT_ERROR)
            {
                retire(client);

                retired_fds.push_back(
                    client->fd);

                continue;
            }

            // Flush existing output first. This can create room for more
            // pipelined input in the same event-loop iteration.
            if (mask & EVENT_WRITE)
            {
                alive =
                    write_client(
                        *client);
            }

            // EV_EOF does not mean unread data is gone. Drain the socket.
            if (alive &&
                (mask &
                 (EVENT_READ |
                  EVENT_EOF)))
            {
                alive =
                    read_client(
                        *client,
                        (mask &
                         EVENT_EOF) != 0);
            }

            if (!alive)
            {
                retire(client);

                retired_fds.push_back(
                    client->fd);

                continue;
            }

            // If reading stopped because output was saturated, consume
            // buffered complete frames after output space has returned.
            if (client->pending_input() &&
                client->pending_output() + 4 <=
                    MAX_OUTPUT_BUFFER)
            {
                if (!process_input_buffer(
                        *client))
                {
                    retire(client);

                    retired_fds.push_back(
                        client->fd);

                    continue;
                }
            }

            // Flush responses generated during this event batch.
            if (client->pending_output() &&
                !write_client(*client))
            {
                retire(client);

                retired_fds.push_back(
                    client->fd);

                continue;
            }

            // Once FIN or close-after-flush is known and all responses
            // have drained, the session can be retired.
            if ((client->peer_closed ||
                 client->close_after_flush) &&
                client->pending_output() == 0)
            {
                retire(client);

                retired_fds.push_back(
                    client->fd);

                continue;
            }

            // READ remains enabled during the normal connection lifetime.
            // WRITE is enabled only while output is pending.
            //
            // After EOF/close-after-flush, READ is disabled to avoid a
            // persistent EOF notification busy-loop on kqueue.
            if (!loop.mod_fd(
                    client->fd,
                    client,
                    !client->peer_closed &&
                        !client->close_after_flush,
                    client->pending_output() != 0))
            {
                retire(client);

                retired_fds.push_back(
                    client->fd);

                continue;
            }
        }

        // ====================================================
        // DEFERRED SESSION DESTRUCTION
        //
        // No ClientSession returned by this kernel batch can be freed
        // until every event in that batch has been classified/processed.
        // ====================================================

        for (int fd :
             retired_fds)
        {
            registered.erase(fd);

            sessions.erase(fd);
        }

        // ====================================================
        // WAKE EVENT
        //
        // Process it after the current client batch. The queue is the
        // authoritative connection state, while the pipe is only a wakeup.
        // ====================================================

        bool wake_seen = false;

#if defined(__linux__)

        for (int i = 0;
             i < ready;
             ++i)
        {
            if (events[i].data.ptr ==
                &wake_tag)
            {
                wake_seen = true;
                break;
            }
        }

#else

        for (int i = 0;
             i < ready;
             ++i)
        {
            if (events[i].udata ==
                &wake_tag)
            {
                wake_seen = true;
                break;
            }
        }

#endif

        if (wake_seen)
        {
            register_pending();
        }
    }

    // ========================================================
    // WORKER SHUTDOWN
    // ========================================================

    for (auto &entry :
         sessions)
    {
        (void)loop.del_fd(
            entry.first);

        entry.second->active =
            false;
    }

    sessions.clear();

    registered.clear();

    close_pending_clients();

    (void)loop.del_fd(
        worker.wake_read);

    loop.close_loop();

    std::cout
        << "[Worker "
        << worker_id
        << "] offline.\n";
}

// ============================================================
// MAIN
// ============================================================

int main()
{
    // ========================================================
    // SIGNALS
    // ========================================================

    struct sigaction sa{};

    sa.sa_handler =
        signal_handler;

    sigemptyset(
        &sa.sa_mask);

    sa.sa_flags = 0;

    if (sigaction(
            SIGINT,
            &sa,
            nullptr) < 0)
    {
        std::cerr
            << "[Fatal error] sigaction(SIGINT) failed: "
            << std::strerror(errno)
            << '\n';

        return 1;
    }

    struct sigaction pipe_sa{};

    pipe_sa.sa_handler =
        SIG_IGN;

    sigemptyset(
        &pipe_sa.sa_mask);

    pipe_sa.sa_flags = 0;

    if (sigaction(
            SIGPIPE,
            &pipe_sa,
            nullptr) < 0)
    {
        std::cerr
            << "[Fatal error] SIGPIPE configuration failed\n";

        return 1;
    }

    // ========================================================
    // SERVER SOCKET
    // ========================================================

    const int server_fd =
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
        close(server_fd);

        std::cerr
            << "[Fatal error] SO_REUSEADDR failed\n";

        return 1;
    }

    if (!set_nonblocking(
            server_fd))
    {
        close(server_fd);

        std::cerr
            << "[Fatal error] listener nonblocking failed\n";

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
            reinterpret_cast<
                sockaddr *>(&address),
            sizeof(address)) < 0)
    {
        close(server_fd);

        std::cerr
            << "[Fatal error] bind() failed: "
            << std::strerror(errno)
            << '\n';

        return 1;
    }

    if (listen(
            server_fd,
            LISTEN_BACKLOG) < 0)
    {
        close(server_fd);

        std::cerr
            << "[Fatal error] listen() failed: "
            << std::strerror(errno)
            << '\n';

        return 1;
    }

    std::cout
        << "Project Ares V3 online... port: "
        << SERVER_PORT
        << '\n';

    std::cout
        << "Architecture: 16-shard LRU, "
           "persistent worker-owned clients, "
           "epoll/kqueue event loops.\n";

    // ========================================================
    // WORKER CONTEXTS
    // ========================================================

    std::vector<WorkerContext>
        worker_contexts(
            NUM_WORKERS);

    // ========================================================
    // WAKE PIPES
    // ========================================================

    for (auto &worker :
         worker_contexts)
    {
        int pipe_fds[2];

        if (pipe(pipe_fds) < 0)
        {
            std::cerr
                << "[Fatal error] pipe() failed: "
                << std::strerror(errno)
                << '\n';

            keep_running = false;

            close(server_fd);

            for (auto &w :
                 worker_contexts)
            {
                if (w.wake_read >= 0)
                {
                    close(w.wake_read);
                }

                if (w.wake_write >= 0)
                {
                    close(w.wake_write);
                }
            }

            return 1;
        }

        worker.wake_read =
            pipe_fds[0];

        worker.wake_write =
            pipe_fds[1];

        if (!set_nonblocking(
                worker.wake_read) ||
            !set_nonblocking(
                worker.wake_write))
        {
            std::cerr
                << "[Fatal error] wake pipe configuration failed\n";

            keep_running = false;

            close(server_fd);

            for (auto &w :
                 worker_contexts)
            {
                if (w.wake_read >= 0)
                {
                    close(w.wake_read);
                }

                if (w.wake_write >= 0)
                {
                    close(w.wake_write);
                }
            }

            return 1;
        }
    }

    // ========================================================
    // WORKERS
    // ========================================================

    std::vector<std::thread>
        workers;

    workers.reserve(
        NUM_WORKERS);

    try
    {
        for (int i = 0;
             i < NUM_WORKERS;
             ++i)
        {
            workers.emplace_back(
                worker_thread,
                std::ref(
                    worker_contexts[i]),
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

        for (auto &worker :
             worker_contexts)
        {
            wake_worker(worker);
        }

        for (auto &worker :
             workers)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }

        close(server_fd);

        for (auto &worker :
             worker_contexts)
        {
            if (worker.wake_read >= 0)
            {
                close(worker.wake_read);
            }

            if (worker.wake_write >= 0)
            {
                close(worker.wake_write);
            }
        }

        return 1;
    }

    // ========================================================
    // ACCEPT LOOP
    // ========================================================

    std::size_t next_worker = 0;

    while (keep_running &&
           !shutdown_requested)
    {
        pollfd listener{
            server_fd,
            POLLIN,
            0};

        const int ready =
            poll(
                &listener,
                1,
                -1);

        if (ready < 0)
        {
            if (errno == EINTR)
            {
                if (shutdown_requested)
                {
                    break;
                }

                continue;
            }

            break;
        }

        if (shutdown_requested)
        {
            break;
        }

        if (listener.revents &
            (POLLERR |
             POLLHUP |
             POLLNVAL))
        {
            break;
        }

        if (!(listener.revents &
              POLLIN))
        {
            continue;
        }

        // Drain pending accepts.
        while (keep_running)
        {
            const int client_socket =
                accept(
                    server_fd,
                    nullptr,
                    nullptr);

            if (client_socket >= 0)
            {
                WorkerContext &worker =
                    worker_contexts[
                        next_worker];

                next_worker =
                    (next_worker + 1) %
                    NUM_WORKERS;

                bool queued = false;

                {
                    std::lock_guard<
                        std::mutex>
                        guard(
                            worker.queue_mutex);

                    if (keep_running)
                    {
                        worker.pending_clients
                            .push(
                                client_socket);

                        queued = true;
                    }
                }

                if (queued)
                {
                    wake_worker(
                        worker);
                }
                else
                {
                    close(
                        client_socket);
                }

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

            if (errno == EAGAIN ||
                errno == EWOULDBLOCK)
            {
                break;
            }

            if (errno == ECONNABORTED)
            {
                continue;
            }

            break;
        }
    }

    // ========================================================
    // SHUTDOWN
    // ========================================================

    keep_running = false;

    close(server_fd);

    for (auto &worker :
         worker_contexts)
    {
        wake_worker(worker);
    }

    std::cout
        << "[Server] Awaiting worker convergence...\n";

    for (auto &worker :
         workers)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    for (auto &worker :
         worker_contexts)
    {
        if (worker.wake_read >= 0)
        {
            close(
                worker.wake_read);

            worker.wake_read = -1;
        }

        if (worker.wake_write >= 0)
        {
            close(
                worker.wake_write);

            worker.wake_write = -1;
        }
    }

    std::cout
        << "[Server] Engine offline. "
           "All memory released safely.\n";

    return 0;
}