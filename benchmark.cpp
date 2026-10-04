#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

// ============================================================
// PROJECT ARES — VALIDATION BENCHMARK
//
// Purpose:
//   1. Validate correctness of the benchmark itself.
//   2. Validate every server response.
//   3. Measure pipelined throughput.
//   4. Show whether throughput scales sensibly with pipeline
//      depth and connection count.
//
// IMPORTANT:
//   This benchmark does NOT assume that "bytes received"
//   means "request succeeded".
//
// GET requests MUST return:
//      OK\n
//
// A GET returning:
//      nil\n
//
// is counted as a FAILURE.
//
// ============================================================

constexpr const char* HOST = "127.0.0.1";
constexpr int PORT = 8080;

constexpr int DEFAULT_CONNECTIONS = 16;
constexpr int DEFAULT_REQUESTS_PER_CONNECTION = 25000;
constexpr int DEFAULT_PIPELINE = 64;

constexpr int GET_KEYSPACE = 100000;
constexpr std::size_t VALUE_SIZE = 32;

constexpr int MAX_PIPELINE = 1024;

// ============================================================
// RESULT
// ============================================================

struct BenchmarkResult {
    std::string name;

    std::uint64_t total_requests = 0;
    std::uint64_t successful = 0;
    std::uint64_t failed = 0;

    std::uint64_t requests_sent = 0;

    std::uint64_t request_bytes = 0;
    std::uint64_t response_bytes = 0;

    double elapsed = 0.0;

    double throughput() const {
        if (elapsed <= 0.0)
            return 0.0;

        return static_cast<double>(successful) / elapsed;
    }

    bool passed() const {
        return failed == 0 &&
               successful == total_requests &&
               requests_sent == total_requests;
    }
};

// ============================================================
// START BARRIER
// ============================================================

class StartBarrier {
    std::mutex mutex_;
    std::condition_variable cv_;

    int participants_;
    int arrived_ = 0;

    bool released_ = false;

    std::chrono::steady_clock::time_point release_time_{};

public:
    explicit StartBarrier(int participants)
        : participants_(participants) {}

    std::chrono::steady_clock::time_point arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);

        ++arrived_;

        if (arrived_ == participants_) {
            release_time_ = std::chrono::steady_clock::now();
            released_ = true;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [&] {
                return released_;
            });
        }

        return release_time_;
    }

    std::chrono::steady_clock::time_point wait_until_released() {
        std::unique_lock<std::mutex> lock(mutex_);

        cv_.wait(lock, [&] {
            return released_;
        });

        return release_time_;
    }
};

// ============================================================
// SOCKET RAII
// ============================================================

class Socket {
    int fd_ = -1;

public:
    Socket() = default;

    explicit Socket(int fd)
        : fd_(fd) {}

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept
        : fd_(other.fd_) {
        other.fd_ = -1;
    }

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();

            fd_ = other.fd_;
            other.fd_ = -1;
        }

        return *this;
    }

    ~Socket() {
        close();
    }

    int get() const {
        return fd_;
    }

    bool valid() const {
        return fd_ >= 0;
    }

    void close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
};

// ============================================================
// CONNECT
// ============================================================

Socket connect_to_server() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0)
        return Socket();

    sockaddr_in addr{};

    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);

    if (::inet_pton(
            AF_INET,
            HOST,
            &addr.sin_addr) <= 0) {
        ::close(fd);
        return Socket();
    }

    while (::connect(
               fd,
               reinterpret_cast<sockaddr*>(&addr),
               sizeof(addr)) < 0) {

        if (errno == EINTR)
            continue;

        ::close(fd);
        return Socket();
    }

    return Socket(fd);
}

// ============================================================
// SEND ALL
// ============================================================

bool send_all(
    int fd,
    const std::uint8_t* data,
    std::size_t length) {

    std::size_t sent = 0;

    while (sent < length) {
        ssize_t n = ::send(
            fd,
            data + sent,
            length - sent,
            MSG_NOSIGNAL);

        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }

        if (n < 0 && errno == EINTR)
            continue;

        return false;
    }

    return true;
}

// ============================================================
// SEND PIPELINE
// ============================================================

bool send_pipeline(
    int fd,
    const std::vector<const std::vector<std::uint8_t>*>& requests,
    std::uint64_t& bytes_sent) {

    if (requests.empty())
        return true;

    std::vector<iovec> iov;
    iov.reserve(requests.size());

    for (const auto* request : requests) {
        iovec entry{};

        entry.iov_base =
            const_cast<std::uint8_t*>(request->data());

        entry.iov_len =
            request->size();

        iov.push_back(entry);
    }

    std::size_t index = 0;
    std::size_t offset = 0;

    while (index < iov.size()) {
        msghdr msg{};

        msg.msg_iov =
            iov.data() + index;

        msg.msg_iovlen =
            iov.size() - index;

        ssize_t n = ::sendmsg(
            fd,
            &msg,
            MSG_NOSIGNAL);

        if (n > 0) {
            bytes_sent +=
                static_cast<std::uint64_t>(n);

            std::size_t remaining =
                static_cast<std::size_t>(n);

            while (remaining > 0 &&
                   index < iov.size()) {

                std::size_t available =
                    iov[index].iov_len - offset;

                if (remaining < available) {
                    offset += remaining;
                    remaining = 0;
                    break;
                }

                remaining -= available;

                ++index;
                offset = 0;
            }

            if (index < iov.size() &&
                offset != 0) {

                iov[index].iov_base =
                    static_cast<char*>(
                        iov[index].iov_base) +
                    offset;

                iov[index].iov_len -= offset;

                offset = 0;
            }

            continue;
        }

        if (n < 0 && errno == EINTR)
            continue;

        return false;
    }

    return true;
}

// ============================================================
// RESPONSE READER
// ============================================================

class ResponseReader {
    std::vector<std::uint8_t> buffer_;
    std::size_t head_ = 0;

public:
    ResponseReader() {
        buffer_.reserve(64 * 1024);
    }

private:
    std::size_t available() const {
        return buffer_.size() - head_;
    }

    void compact() {
        if (head_ == 0)
            return;

        std::size_t remaining =
            available();

        if (remaining > 0) {
            std::memmove(
                buffer_.data(),
                buffer_.data() + head_,
                remaining);
        }

        buffer_.resize(remaining);
        head_ = 0;
    }

    bool fill(int fd) {
        if (head_ > 0 &&
            (head_ >= 32 * 1024 ||
             buffer_.size() == buffer_.capacity())) {
            compact();
        }

        std::uint8_t temp[64 * 1024];

        while (true) {
            ssize_t n = ::recv(
                fd,
                temp,
                sizeof(temp),
                0);

            if (n > 0) {
                buffer_.insert(
                    buffer_.end(),
                    temp,
                    temp + n);

                return true;
            }

            if (n < 0 && errno == EINTR)
                continue;

            return false;
        }
    }

public:
    // --------------------------------------------------------
    // Strict SET response
    // --------------------------------------------------------

    bool consume_set(
        int fd,
        std::uint64_t& response_bytes) {

        while (available() < 3) {
            if (!fill(fd))
                return false;
        }

        const auto* p =
            buffer_.data() + head_;

        if (p[0] != 'O' ||
            p[1] != 'K' ||
            p[2] != '\n') {

            return false;
        }

        head_ += 3;
        response_bytes += 3;

        return true;
    }

    // --------------------------------------------------------
    // Strict GET response
    //
    // A cache miss is NOT success.
    // --------------------------------------------------------

    bool consume_get(
        int fd,
        std::uint64_t& response_bytes) {

        while (available() < 3) {
            if (!fill(fd))
                return false;
        }

        const auto* p =
            buffer_.data() + head_;

        // Expected cache hit.
        if (p[0] == 'O' &&
            p[1] == 'K' &&
            p[2] == '\n') {

            head_ += 3;
            response_bytes += 3;

            return true;
        }

        // A nil response means the benchmark's expected
        // cache state is wrong.
        if (p[0] == 'n' &&
            p[1] == 'i' &&
            p[2] == 'l') {

            while (available() < 4) {
                if (!fill(fd))
                    return false;
            }

            if (buffer_[head_ + 3] != '\n')
                return false;

            // Consume it so the stream remains synchronized.
            head_ += 4;
            response_bytes += 4;

            return false;
        }

        return false;
    }
};

// ============================================================
// REQUEST BUILDERS
// ============================================================

std::vector<std::uint8_t>
build_set(int key_id) {

    const std::string key =
        "bench_key_" +
        std::to_string(key_id);

    const std::string value(
        VALUE_SIZE,
        'A');

    std::vector<std::uint8_t> request(
        9 +
        key.size() +
        value.size());

    request[0] = 0x01;

    std::uint32_t key_len =
        htonl(
            static_cast<std::uint32_t>(
                key.size()));

    std::uint32_t value_len =
        htonl(
            static_cast<std::uint32_t>(
                value.size()));

    std::memcpy(
        request.data() + 1,
        &key_len,
        4);

    std::memcpy(
        request.data() + 5,
        &value_len,
        4);

    std::memcpy(
        request.data() + 9,
        key.data(),
        key.size());

    std::memcpy(
        request.data() +
            9 +
            key.size(),
        value.data(),
        value.size());

    return request;
}

std::vector<std::uint8_t>
build_get(int key_id) {

    const std::string key =
        "bench_key_" +
        std::to_string(key_id);

    std::vector<std::uint8_t> request(
        9 + key.size());

    request[0] = 0x02;

    std::uint32_t key_len =
        htonl(
            static_cast<std::uint32_t>(
                key.size()));

    std::uint32_t value_len = 0;

    std::memcpy(
        request.data() + 1,
        &key_len,
        4);

    std::memcpy(
        request.data() + 5,
        &value_len,
        4);

    std::memcpy(
        request.data() + 9,
        key.data(),
        key.size());

    return request;
}

// ============================================================
// REQUEST SET
// ============================================================

struct Requests {
    std::vector<
        std::vector<std::uint8_t>>
        sets;

    std::vector<
        std::vector<std::uint8_t>>
        gets;
};

Requests build_requests() {

    Requests r;

    std::cout
        << "Preparing benchmark requests...\n";

    r.sets.reserve(GET_KEYSPACE);
    r.gets.reserve(GET_KEYSPACE);

    for (int i = 0;
         i < GET_KEYSPACE;
         ++i) {

        r.sets.push_back(
            build_set(i));

        r.gets.push_back(
            build_get(i));
    }

    return r;
}

// ============================================================
// WARM CACHE
// ============================================================

bool warm_cache(
    const Requests& requests) {

    std::cout
        << "\nWarming "
        << GET_KEYSPACE
        << " keys...\n";

    Socket socket =
        connect_to_server();

    if (!socket.valid()) {
        std::cerr
            << "Warmup connection failed.\n";

        return false;
    }

    ResponseReader reader;

    std::uint64_t bytes = 0;

    for (int i = 0;
         i < GET_KEYSPACE;
         ++i) {

        const auto& req =
            requests.sets[i];

        if (!send_all(
                socket.get(),
                req.data(),
                req.size())) {

            return false;
        }

        if (!reader.consume_set(
                socket.get(),
                bytes)) {

            return false;
        }
    }

    std::cout
        << "Warmup complete.\n";

    return true;
}

// ============================================================
// COUNTERS
// ============================================================

struct Counters {
    std::uint64_t success = 0;
    std::uint64_t failure = 0;

    std::uint64_t requests_sent = 0;

    std::uint64_t request_bytes = 0;
    std::uint64_t response_bytes = 0;
};

// ============================================================
// PIPELINED TEST
// ============================================================

enum class Mode {
    SET,
    GET
};

BenchmarkResult run_pipelined(
    const Requests& requests,
    int connections,
    int requests_per_connection,
    int pipeline,
    Mode mode) {

    BenchmarkResult result;

    result.name =
        mode == Mode::SET
            ? "Pipelined SET"
            : "Pipelined GET";

    result.total_requests =
        static_cast<std::uint64_t>(
            connections) *
        requests_per_connection;

    StartBarrier barrier(
        connections);

    std::vector<std::thread> workers;

    std::vector<Counters>
        counters(connections);

    workers.reserve(connections);

    for (int thread_id = 0;
         thread_id < connections;
         ++thread_id) {

        workers.emplace_back(
            [&requests,
             &barrier,
             &counters,
             thread_id,
             connections,
             requests_per_connection,
             pipeline,
             mode]() {

                Socket socket =
                    connect_to_server();

                if (!socket.valid()) {
                    counters[thread_id].failure =
                        requests_per_connection;

                    barrier.arrive_and_wait();
                    return;
                }

                // ALL connections established here.
                // Timing begins only after every worker arrives.
                barrier.arrive_and_wait();

                Counters& c =
                    counters[thread_id];

                ResponseReader reader;

                int completed = 0;

                while (completed <
                       requests_per_connection) {

                    int batch =
                        std::min(
                            pipeline,
                            requests_per_connection -
                                completed);

                    std::vector<
                        const std::vector<std::uint8_t>*>
                        batch_requests;

                    batch_requests.reserve(
                        batch);

                    for (int j = 0;
                         j < batch;
                         ++j) {

                        int sequence =
                            completed + j;

                        int key =
                            (sequence +
                             thread_id * 997)
                            % GET_KEYSPACE;

                        if (mode == Mode::SET) {

                            batch_requests.push_back(
                                &requests.sets[key]);

                        } else {

                            batch_requests.push_back(
                                &requests.gets[key]);
                        }
                    }

                    if (!send_pipeline(
                            socket.get(),
                            batch_requests,
                            c.request_bytes)) {

                        c.failure +=
                            requests_per_connection -
                            completed;

                        break;
                    }

                    c.requests_sent +=
                        batch;

                    int responses = 0;

                    for (int j = 0;
                         j < batch;
                         ++j) {

                        bool ok;

                        if (mode == Mode::SET) {

                            ok =
                                reader.consume_set(
                                    socket.get(),
                                    c.response_bytes);

                        } else {

                            ok =
                                reader.consume_get(
                                    socket.get(),
                                    c.response_bytes);
                        }

                        if (!ok) {

                            // Current response failed.
                            // Remaining responses are also
                            // considered failed because we
                            // cannot safely claim completion.

                            c.failure +=
                                requests_per_connection -
                                (completed + responses);

                            return;
                        }

                        ++responses;
                        ++c.success;
                    }

                    completed += batch;
                }
            });
    }

    const auto start =
        barrier.wait_until_released();

    for (auto& t : workers)
        t.join();

    const auto end =
        std::chrono::steady_clock::now();

    result.elapsed =
        std::chrono::duration<double>(
            end - start).count();

    for (const Counters& c :
         counters) {

        result.successful +=
            c.success;

        result.failed +=
            c.failure;

        result.requests_sent +=
            c.requests_sent;

        result.request_bytes +=
            c.request_bytes;

        result.response_bytes +=
            c.response_bytes;
    }

    return result;
}

// ============================================================
// PRINT RESULT
// ============================================================

void print_result(
    const BenchmarkResult& r) {

    std::cout
        << "\n--------------------------------------------------\n"
        << r.name << '\n'
        << "Requests:            "
        << r.total_requests << '\n'
        << "Requests sent:       "
        << r.requests_sent << '\n'
        << "Successful:          "
        << r.successful << '\n'
        << "Failed:              "
        << r.failed << '\n'
        << "Request bytes:       "
        << r.request_bytes << '\n'
        << "Response bytes:      "
        << r.response_bytes << '\n'
        << "Elapsed:             "
        << std::fixed
        << std::setprecision(6)
        << r.elapsed
        << " s\n"
        << "Throughput:          "
        << std::fixed
        << std::setprecision(2)
        << r.throughput()
        << " req/s\n";

    if (r.elapsed > 0.0) {

        double mbps =
            static_cast<double>(
                r.request_bytes +
                r.response_bytes)
            / r.elapsed
            / 1000000.0;

        std::cout
            << "Wire throughput:     "
            << std::fixed
            << std::setprecision(2)
            << mbps
            << " MB/s\n";
    }

    std::cout
        << "Result:              "
        << (r.passed()
                ? "PASS"
                : "FAIL")
        << '\n'
        << "--------------------------------------------------\n";
}

// ============================================================
// SANITY CHECK
// ============================================================

bool validate(
    const BenchmarkResult& r) {

    if (!r.passed()) {

        std::cerr
            << "\n!!! BENCHMARK VALIDATION FAILED !!!\n"
            << "The throughput number MUST NOT be used.\n";

        return false;
    }

    return true;
}

// ============================================================
// PIPELINE SWEEP
// ============================================================

void pipeline_sweep(
    const Requests& requests,
    int connections,
    int requests_per_connection) {

    const int pipelines[] = {
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256
    };

    std::cout
        << "\n==================================================\n"
        << "             PIPELINE DEPTH SWEEP\n"
        << "==================================================\n"
        << "Connections: "
        << connections
        << "\n\n";

    for (int p : pipelines) {

        auto r =
            run_pipelined(
                requests,
                connections,
                requests_per_connection,
                p,
                Mode::GET);

        std::cout
            << "Pipeline "
            << std::setw(3)
            << p
            << " : "
            << std::setw(10)
            << std::fixed
            << std::setprecision(0)
            << r.throughput()
            << " req/s";

        if (!r.passed())
            std::cout << "   FAIL";

        std::cout << '\n';
    }
}

// ============================================================
// CONNECTION SWEEP
// ============================================================

void connection_sweep(
    const Requests& requests,
    int requests_per_connection) {

    const int connections[] = {
        1,
        2,
        4,
        8,
        16,
        32
    };

    constexpr int PIPELINE = 64;

    std::cout
        << "\n==================================================\n"
        << "             CONNECTION COUNT SWEEP\n"
        << "==================================================\n"
        << "Pipeline depth: "
        << PIPELINE
        << "\n\n";

    for (int c : connections) {

        auto r =
            run_pipelined(
                requests,
                c,
                requests_per_connection,
                PIPELINE,
                Mode::GET);

        std::cout
            << "Connections "
            << std::setw(2)
            << c
            << " : "
            << std::setw(10)
            << std::fixed
            << std::setprecision(0)
            << r.throughput()
            << " req/s";

        if (!r.passed())
            std::cout << "   FAIL";

        std::cout << '\n';
    }
}

// ============================================================
// MAIN
// ============================================================

int main() {

    std::cout
        << "==================================================\n"
        << "       PROJECT ARES — VALIDATION BENCHMARK\n"
        << "==================================================\n"
        << "Target:              "
        << HOST << ':' << PORT << '\n'
        << "Connections:         "
        << DEFAULT_CONNECTIONS << '\n'
        << "Requests/connection: "
        << DEFAULT_REQUESTS_PER_CONNECTION << '\n'
        << "Pipeline:            "
        << DEFAULT_PIPELINE << '\n'
        << "Keyspace:            "
        << GET_KEYSPACE << '\n'
        << "==================================================\n";

    Requests requests =
        build_requests();

    // --------------------------------------------------------
    // Establish known-good cache state.
    // --------------------------------------------------------

    if (!warm_cache(requests))
        return 1;

    // --------------------------------------------------------
    // MAIN SET TEST
    // --------------------------------------------------------

    std::cout
        << "\n[1/2] PIPELINED SET\n";

    auto set_result =
        run_pipelined(
            requests,
            DEFAULT_CONNECTIONS,
            DEFAULT_REQUESTS_PER_CONNECTION,
            DEFAULT_PIPELINE,
            Mode::SET);

    print_result(set_result);

    if (!validate(set_result))
        return 2;

    // --------------------------------------------------------
    // Re-warm cache because SET test changed values.
    // --------------------------------------------------------

    if (!warm_cache(requests))
        return 1;

    // --------------------------------------------------------
    // MAIN GET TEST
    // --------------------------------------------------------

    std::cout
        << "\n[2/2] PIPELINED GET\n";

    auto get_result =
        run_pipelined(
            requests,
            DEFAULT_CONNECTIONS,
            DEFAULT_REQUESTS_PER_CONNECTION,
            DEFAULT_PIPELINE,
            Mode::GET);

    print_result(get_result);

    if (!validate(get_result))
        return 3;

    // --------------------------------------------------------
    // Sanity sweeps
    // --------------------------------------------------------

    pipeline_sweep(
        requests,
        DEFAULT_CONNECTIONS,
        DEFAULT_REQUESTS_PER_CONNECTION);

    connection_sweep(
        requests,
        DEFAULT_REQUESTS_PER_CONNECTION);

    // --------------------------------------------------------
    // Final verdict
    // --------------------------------------------------------

    std::cout
        << "\n==================================================\n"
        << "                 VALIDATION RESULT\n"
        << "==================================================\n";

    if (set_result.passed() &&
        get_result.passed()) {

        std::cout
            << "Application correctness: PASS\n"
            << "Request accounting:      PASS\n"
            << "Response accounting:     PASS\n"
            << "GET cache-hit validation: PASS\n"
            << "Pipeline test:           PASS\n"
            << "--------------------------------------------------\n"
            << "Pipelined SET: "
            << std::fixed
            << std::setprecision(2)
            << set_result.throughput()
            << " req/s\n"
            << "Pipelined GET: "
            << get_result.throughput()
            << " req/s\n"
            << "==================================================\n";

    } else {

        std::cout
            << "VALIDATION FAILED\n"
            << "Do NOT report the throughput.\n"
            << "==================================================\n";

        return 4;
    }

    return 0;
}
/*
--------------------------------------------------
Pipelined GET
Requests:            400000
Requests sent:       400000
Successful:          400000
Failed:              0
Request bytes:       9543722
Response bytes:      1200000
Elapsed:             0.049509 s
Throughput:          8079373.05 req/s
Wire throughput:     217.01 MB/s
Result:              PASS
--------------------------------------------------

==================================================
             PIPELINE DEPTH SWEEP
==================================================
Connections: 16

Pipeline   1 :     170194 req/s
Pipeline   2 :     339020 req/s
Pipeline   4 :     686719 req/s
Pipeline   8 :    1341226 req/s
Pipeline  16 :    2631319 req/s
Pipeline  32 :    4955000 req/s
Pipeline  64 :    7897666 req/s
Pipeline 128 :   10844860 req/s
Pipeline 256 :   13512049 req/s

==================================================
             CONNECTION COUNT SWEEP
==================================================
Pipeline depth: 64

Connections  1 :    2945089 req/s
Connections  2 :    5006487 req/s
Connections  4 :    6827570 req/s
Connections  8 :    7544845 req/s
Connections 16 :    8000593 req/s
Connections 32 :    8125887 req/s

==================================================
                 VALIDATION RESULT
==================================================
Application correctness: PASS
Request accounting:      PASS
Response accounting:     PASS
GET cache-hit validation: PASS
Pipeline test:           PASS
--------------------------------------------------
Pipelined SET: 5855611.57 req/s
Pipelined GET: 8079373.05 req/s
==================================================
sakshampal@SAKSHAMS-M5 project-ares % 
*/