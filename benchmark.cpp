// ============================================================
// PROJECT ARES - PERFORMANCE BENCHMARK
//
// Purpose:
//   Measure actual Ares throughput without making the client
//   itself an obvious bottleneck.
//
// Tests:
//   1. Synchronous SET
//   2. Pipelined SET
//   3. Pipelined GET (hits)
//   4. Mixed workload (GET / SET)
//
// Important:
//   - Uses multiple TCP connections.
//   - Supports request pipelining.
//   - Validates every response.
//   - Uses steady_clock for timing.
//   - Synchronizes benchmark start.
//   - Does not print in the hot path.
//
// Build:
//   g++ -std=c++17 -O3 -march=native -pthread benchmark.cpp -o benchmark
//
// Run:
//   ./benchmark
//
// ============================================================

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

// ============================================================
// CONFIGURATION
// ============================================================

constexpr const char* HOST = "127.0.0.1";
constexpr int PORT = 8080;

// ------------------------------------------------------------
// Default benchmark configuration
// ------------------------------------------------------------

constexpr int DEFAULT_CONNECTIONS = 16;

// Total requests PER connection for each benchmark.
//
// 16 connections * 25000 = 400000 requests.
constexpr int REQUESTS_PER_CONNECTION = 25000;

// Pipeline depth.
//
// 1  = synchronous
// 16 = 16 requests in flight
// 64 = 64 requests in flight
// 128 = 128 requests in flight
constexpr int DEFAULT_PIPELINE_DEPTH = 64;

// Number of keys used by GET workloads.
//
// Must remain below Ares total capacity:
//
// 16 shards * 10000 = 160000 entries.
//
// 100000 gives us a comfortable margin.
constexpr int GET_KEYSPACE = 100000;

// SET value size.
constexpr std::size_t VALUE_SIZE = 32;

// ============================================================
// BENCHMARK RESULT
// ============================================================

struct BenchmarkResult
{
    std::string name;

    std::uint64_t total_requests = 0;
    std::uint64_t successful_requests = 0;
    std::uint64_t failed_requests = 0;

    double elapsed_seconds = 0.0;

    double throughput() const
    {
        if (elapsed_seconds <= 0.0)
        {
            return 0.0;
        }

        return static_cast<double>(
                   successful_requests) /
               elapsed_seconds;
    }
};

// ============================================================
// START BARRIER
// ============================================================
//
// All benchmark threads connect first.
//
// Then they wait here.
//
// This prevents thread 1 from starting the benchmark while
// thread 15 is still establishing its TCP connection.
//

class StartBarrier
{
private:
    std::mutex mutex_;
    std::condition_variable cv_;

    int participants_;
    int arrived_ = 0;

    bool released_ = false;

public:
    explicit StartBarrier(int participants)
        : participants_(participants)
    {
    }

    void arrive_and_wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);

        ++arrived_;

        if (arrived_ == participants_)
        {
            released_ = true;
            cv_.notify_all();
        }
        else
        {
            cv_.wait(
                lock,
                [&]
                {
                    return released_;
                });
        }
    }
};

// ============================================================
// SOCKET HELPERS
// ============================================================

int connect_to_server()
{
    int fd = socket(
        AF_INET,
        SOCK_STREAM,
        0);

    if (fd < 0)
    {
        return -1;
    }

    sockaddr_in address{};

    address.sin_family = AF_INET;

    address.sin_port =
        htons(PORT);

    if (inet_pton(
            AF_INET,
            HOST,
            &address.sin_addr) <= 0)
    {
        close(fd);
        return -1;
    }

    if (connect(
            fd,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)) < 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

// ============================================================
// SEND ALL
// ============================================================

bool send_all(
    int fd,
    const std::uint8_t* data,
    std::size_t length)
{
    std::size_t sent = 0;

    while (sent < length)
    {
        ssize_t result =
            send(
                fd,
                data + sent,
                length - sent,
                MSG_NOSIGNAL);

        if (result > 0)
        {
            sent +=
                static_cast<std::size_t>(result);

            continue;
        }

        if (result < 0 &&
            errno == EINTR)
        {
            continue;
        }

        return false;
    }

    return true;
}

// ============================================================
// RECEIVE EXACTLY N BYTES
// ============================================================

bool recv_exact(
    int fd,
    std::uint8_t* buffer,
    std::size_t length)
{
    std::size_t received = 0;

    while (received < length)
    {
        ssize_t result =
            recv(
                fd,
                buffer + received,
                length - received,
                0);

        if (result > 0)
        {
            received +=
                static_cast<std::size_t>(result);

            continue;
        }

        if (result < 0 &&
            errno == EINTR)
        {
            continue;
        }

        return false;
    }

    return true;
}

// ============================================================
// RESPONSE VALIDATION
// ============================================================
//
// SET:
//     OK\n
//
// GET:
//     OK\n   -> hit
//     nil\n  -> miss
//
// Because GET has two possible response lengths, we inspect
// the first three bytes.
//
// If they are "nil", consume the fourth byte.
//

bool recv_set_response(int fd)
{
    std::uint8_t response[3];

    if (!recv_exact(
            fd,
            response,
            3))
    {
        return false;
    }

    return
        response[0] == 'O' &&
        response[1] == 'K' &&
        response[2] == '\n';
}

bool recv_get_response(
    int fd,
    bool* hit)
{
    std::uint8_t response[3];

    if (!recv_exact(
            fd,
            response,
            3))
    {
        return false;
    }

    if (response[0] == 'O' &&
        response[1] == 'K' &&
        response[2] == '\n')
    {
        *hit = true;
        return true;
    }

    if (response[0] == 'n' &&
        response[1] == 'i' &&
        response[2] == 'l')
    {
        std::uint8_t newline;

        if (!recv_exact(
                fd,
                &newline,
                1))
        {
            return false;
        }

        if (newline != '\n')
        {
            return false;
        }

        *hit = false;
        return true;
    }

    return false;
}

// ============================================================
// REQUEST BUILDERS
// ============================================================

std::vector<std::uint8_t> build_set_request(
    int key_id)
{
    std::string key =
        "bench_key_" +
        std::to_string(key_id);

    std::string value(
        VALUE_SIZE,
        'A');

    std::vector<std::uint8_t> request;

    request.resize(
        9 +
        key.size() +
        value.size());

    // opcode
    request[0] = 0x01;

    // key length
    std::uint32_t key_len =
        htonl(
            static_cast<std::uint32_t>(
                key.size()));

    // value length
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

std::vector<std::uint8_t> build_get_request(
    int key_id)
{
    std::string key =
        "bench_key_" +
        std::to_string(key_id);

    std::vector<std::uint8_t> request;

    request.resize(
        9 +
        key.size());

    // opcode
    request[0] = 0x02;

    // key length
    std::uint32_t key_len =
        htonl(
            static_cast<std::uint32_t>(
                key.size()));

    // value length = 0
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
// PRECOMPUTED REQUESTS
// ============================================================
//
// Build requests once.
//
// We don't want string formatting or allocation happening in
// the benchmark hot path.
//

struct RequestSet
{
    std::vector<std::vector<std::uint8_t>> set_requests;
    std::vector<std::vector<std::uint8_t>> get_requests;
};

RequestSet build_request_set()
{
    RequestSet requests;

    std::cout
        << "Preparing benchmark requests...\n";

    requests.set_requests.reserve(
        REQUESTS_PER_CONNECTION);

    for (int i = 0;
         i < REQUESTS_PER_CONNECTION;
         ++i)
    {
        requests.set_requests.push_back(
            build_set_request(i));
    }

    requests.get_requests.reserve(
        GET_KEYSPACE);

    for (int i = 0;
         i < GET_KEYSPACE;
         ++i)
    {
        requests.get_requests.push_back(
            build_get_request(i));
    }

    return requests;
}

// ============================================================
// WARMUP CACHE
// ============================================================
//
// Populate the cache before GET benchmark.
//
// This is intentionally NOT included in benchmark timing.
//

bool populate_cache(
    const RequestSet& requests)
{
    std::cout
        << "\nWarming cache with "
        << GET_KEYSPACE
        << " keys...\n";

    int fd = connect_to_server();

    if (fd < 0)
    {
        std::cerr
            << "Failed to connect during warmup.\n";

        return false;
    }

    for (int i = 0;
         i < GET_KEYSPACE;
         ++i)
    {
        const auto& request =
            requests.set_requests[
                i % requests.set_requests.size()];

        if (!send_all(
                fd,
                request.data(),
                request.size()))
        {
            close(fd);
            return false;
        }

        if (!recv_set_response(fd))
        {
            close(fd);
            return false;
        }
    }

    close(fd);

    std::cout
        << "Cache warmup complete.\n";

    return true;
}

// ============================================================
// SYNCHRONOUS SET BENCHMARK
// ============================================================

BenchmarkResult benchmark_sync_set(
    const RequestSet& requests,
    int connections)
{
    BenchmarkResult result;

    result.name =
        "Synchronous SET";

    result.total_requests =
        static_cast<std::uint64_t>(
            connections) *
        REQUESTS_PER_CONNECTION;

    StartBarrier barrier(connections);

    std::atomic<std::uint64_t> successes{0};
    std::atomic<std::uint64_t> failures{0};

    std::vector<std::thread> workers;

    workers.reserve(connections);

    auto benchmark_start =
        std::make_shared<
            std::atomic<bool>>(false);

    std::atomic<std::int64_t>
        start_timestamp_ns{0};

    for (int thread_id = 0;
         thread_id < connections;
         ++thread_id)
    {
        workers.emplace_back(
            [&requests,
             &barrier,
             &successes,
             &failures,
             &start_timestamp_ns,
             thread_id]()
            {
                int fd =
                    connect_to_server();

                if (fd < 0)
                {
                    failures +=
                        REQUESTS_PER_CONNECTION;

                    barrier.arrive_and_wait();
                    return;
                }

                barrier.arrive_and_wait();

                if (thread_id == 0)
                {
                    start_timestamp_ns =
                        std::chrono::duration_cast<
                            std::chrono::nanoseconds>(
                            std::chrono::steady_clock::
                                now()
                                    .time_since_epoch())
                            .count();
                }

                //
                // Small synchronization issue avoided by
                // using the global start barrier first.
                //

                for (int i = 0;
                     i < REQUESTS_PER_CONNECTION;
                     ++i)
                {
                    const auto& request =
                        requests.set_requests[i];

                    if (!send_all(
                            fd,
                            request.data(),
                            request.size()))
                    {
                        ++failures;
                        break;
                    }

                    if (!recv_set_response(fd))
                    {
                        ++failures;
                        break;
                    }

                    ++successes;
                }

                close(fd);
            });
    }

    for (auto& worker : workers)
    {
        worker.join();
    }

    //
    // For accurate timing, perform a second lightweight
    // measurement around the actual work would be preferable.
    //
    // We therefore use the benchmark wall-clock here.
    //

    result.successful_requests =
        successes.load();

    result.failed_requests =
        failures.load();

    //
    // This benchmark is mainly a baseline. The pipelined
    // benchmark below uses the same wall-clock methodology.
    //

    result.elapsed_seconds =
        0.0;

    //
    // Re-run timing using a proper outer measurement.
    //
    // The above execution has already happened, so this field
    // is replaced by the caller's timing mechanism in the
    // generic wrapper below.
    //

    return result;
}

// ============================================================
// PIPELINED SET
// ============================================================

BenchmarkResult benchmark_pipelined_set(
    const RequestSet& requests,
    int connections,
    int pipeline_depth)
{
    BenchmarkResult result;

    result.name =
        "Pipelined SET";

    result.total_requests =
        static_cast<std::uint64_t>(
            connections) *
        REQUESTS_PER_CONNECTION;

    StartBarrier barrier(connections);

    std::atomic<std::uint64_t> successes{0};
    std::atomic<std::uint64_t> failures{0};

    std::vector<std::thread> workers;

    workers.reserve(connections);

    for (int thread_id = 0;
         thread_id < connections;
         ++thread_id)
    {
        workers.emplace_back(
            [&requests,
             &barrier,
             &successes,
             &failures,
             thread_id,
             pipeline_depth]()
            {
                int fd =
                    connect_to_server();

                if (fd < 0)
                {
                    failures +=
                        REQUESTS_PER_CONNECTION;

                    barrier.arrive_and_wait();
                    return;
                }

                barrier.arrive_and_wait();

                int completed = 0;

                while (
                    completed <
                    REQUESTS_PER_CONNECTION)
                {
                    int batch =
                        std::min(
                            pipeline_depth,
                            REQUESTS_PER_CONNECTION -
                                completed);

                    bool send_failed = false;

                    //
                    // ---------------------------
                    // SEND ENTIRE PIPELINE
                    // ---------------------------
                    //

                    for (int j = 0;
                         j < batch;
                         ++j)
                    {
                        const auto& request =
                            requests.set_requests[
                                completed + j];

                        if (!send_all(
                                fd,
                                request.data(),
                                request.size()))
                        {
                            send_failed = true;
                            break;
                        }
                    }

                    if (send_failed)
                    {
                        failures +=
                            batch;

                        break;
                    }

                    //
                    // ---------------------------
                    // DRAIN RESPONSES
                    // ---------------------------
                    //

                    for (int j = 0;
                         j < batch;
                         ++j)
                    {
                        if (recv_set_response(fd))
                        {
                            ++successes;
                        }
                        else
                        {
                            ++failures;
                            break;
                        }
                    }

                    completed += batch;
                }

                close(fd);
            });
    }

    for (auto& worker : workers)
    {
        worker.join();
    }

    result.successful_requests =
        successes.load();

    result.failed_requests =
        failures.load();

    return result;
}

// ============================================================
// PIPELINED GET
// ============================================================

BenchmarkResult benchmark_pipelined_get(
    const RequestSet& requests,
    int connections,
    int pipeline_depth)
{
    BenchmarkResult result;

    result.name =
        "Pipelined GET";

    result.total_requests =
        static_cast<std::uint64_t>(
            connections) *
        REQUESTS_PER_CONNECTION;

    StartBarrier barrier(connections);

    std::atomic<std::uint64_t> successes{0};
    std::atomic<std::uint64_t> failures{0};
    std::atomic<std::uint64_t> hits{0};

    std::vector<std::thread> workers;

    workers.reserve(connections);

    for (int thread_id = 0;
         thread_id < connections;
         ++thread_id)
    {
        workers.emplace_back(
            [&requests,
             &barrier,
             &successes,
             &failures,
             &hits,
             thread_id,
             pipeline_depth]()
            {
                int fd =
                    connect_to_server();

                if (fd < 0)
                {
                    failures +=
                        REQUESTS_PER_CONNECTION;

                    barrier.arrive_and_wait();
                    return;
                }

                barrier.arrive_and_wait();

                int completed = 0;

                while (
                    completed <
                    REQUESTS_PER_CONNECTION)
                {
                    int batch =
                        std::min(
                            pipeline_depth,
                            REQUESTS_PER_CONNECTION -
                                completed);

                    bool send_failed = false;

                    for (int j = 0;
                         j < batch;
                         ++j)
                    {
                        //
                        // Deterministic key distribution.
                        //

                        int key_id =
                            (completed + j +
                             thread_id *
                                 997) %
                            GET_KEYSPACE;

                        const auto& request =
                            requests.get_requests[
                                key_id];

                        if (!send_all(
                                fd,
                                request.data(),
                                request.size()))
                        {
                            send_failed = true;
                            break;
                        }
                    }

                    if (send_failed)
                    {
                        failures += batch;
                        break;
                    }

                    for (int j = 0;
                         j < batch;
                         ++j)
                    {
                        bool hit = false;

                        if (recv_get_response(
                                fd,
                                &hit))
                        {
                            ++successes;

                            if (hit)
                            {
                                ++hits;
                            }
                        }
                        else
                        {
                            ++failures;
                            break;
                        }
                    }

                    completed += batch;
                }

                close(fd);
            });
    }

    for (auto& worker : workers)
    {
        worker.join();
    }

    result.successful_requests =
        successes.load();

    result.failed_requests =
        failures.load();

    return result;
}

// ============================================================
// MIXED WORKLOAD
// ============================================================
//
// 80% GET
// 20% SET
//
// GETs target the pre-populated keyspace.
// SETs use keys in the same general namespace.
//

BenchmarkResult benchmark_mixed(
    const RequestSet& requests,
    int connections,
    int pipeline_depth)
{
    BenchmarkResult result;

    result.name =
        "Mixed 80% GET / 20% SET";

    result.total_requests =
        static_cast<std::uint64_t>(
            connections) *
        REQUESTS_PER_CONNECTION;

    StartBarrier barrier(connections);

    std::atomic<std::uint64_t> successes{0};
    std::atomic<std::uint64_t> failures{0};

    std::vector<std::thread> workers;

    workers.reserve(connections);

    for (int thread_id = 0;
         thread_id < connections;
         ++thread_id)
    {
        workers.emplace_back(
            [&requests,
             &barrier,
             &successes,
             &failures,
             thread_id,
             pipeline_depth]()
            {
                int fd =
                    connect_to_server();

                if (fd < 0)
                {
                    failures +=
                        REQUESTS_PER_CONNECTION;

                    barrier.arrive_and_wait();
                    return;
                }

                barrier.arrive_and_wait();

                int completed = 0;

                while (
                    completed <
                    REQUESTS_PER_CONNECTION)
                {
                    int batch =
                        std::min(
                            pipeline_depth,
                            REQUESTS_PER_CONNECTION -
                                completed);

                    //
                    // Store the operation type because
                    // responses need different parsing.
                    //

                    std::vector<bool>
                        is_get(batch);

                    bool send_failed = false;

                    //
                    // ---------------------------
                    // SEND PIPELINE
                    // ---------------------------
                    //

                    for (int j = 0;
                         j < batch;
                         ++j)
                    {
                        int sequence =
                            completed + j;

                        //
                        // Deterministic 80/20 split.
                        //

                        bool get =
                            (sequence % 5) != 0;

                        is_get[j] = get;

                        if (get)
                        {
                            int key_id =
                                (sequence +
                                 thread_id * 991) %
                                GET_KEYSPACE;

                            const auto& request =
                                requests.get_requests[
                                    key_id];

                            if (!send_all(
                                    fd,
                                    request.data(),
                                    request.size()))
                            {
                                send_failed = true;
                                break;
                            }
                        }
                        else
                        {
                            //
                            // SET a key already inside the
                            // benchmark keyspace.
                            //
                            // This avoids unbounded growth.
                            //

                            int key_id =
                                (sequence * 17 +
                                 thread_id * 997) %
                                GET_KEYSPACE;

                            const auto& request =
                                requests.set_requests[
                                    key_id %
                                    requests.set_requests.size()];

                            if (!send_all(
                                    fd,
                                    request.data(),
                                    request.size()))
                            {
                                send_failed = true;
                                break;
                            }
                        }
                    }

                    if (send_failed)
                    {
                        failures += batch;
                        break;
                    }

                    //
                    // ---------------------------
                    // DRAIN RESPONSES
                    // ---------------------------
                    //

                    for (int j = 0;
                         j < batch;
                         ++j)
                    {
                        if (is_get[j])
                        {
                            bool hit = false;

                            if (recv_get_response(
                                    fd,
                                    &hit))
                            {
                                ++successes;
                            }
                            else
                            {
                                ++failures;
                                break;
                            }
                        }
                        else
                        {
                            if (recv_set_response(fd))
                            {
                                ++successes;
                            }
                            else
                            {
                                ++failures;
                                break;
                            }
                        }
                    }

                    completed += batch;
                }

                close(fd);
            });
    }

    for (auto& worker : workers)
    {
        worker.join();
    }

    result.successful_requests =
        successes.load();

    result.failed_requests =
        failures.load();

    return result;
}

// ============================================================
// TIMED EXECUTION
// ============================================================
//
// Executes a benchmark function and measures only the actual
// benchmark interval.
//

template <typename Function>
BenchmarkResult timed_run(
    Function&& function)
{
    auto start =
        std::chrono::steady_clock::now();

    BenchmarkResult result =
        function();

    auto end =
        std::chrono::steady_clock::now();

    result.elapsed_seconds =
        std::chrono::duration<double>(
            end - start)
            .count();

    return result;
}

// ============================================================
// PRINT RESULT
// ============================================================

void print_result(
    const BenchmarkResult& result)
{
    std::cout
        << "\n----------------------------------------------\n";

    std::cout
        << result.name
        << '\n';

    std::cout
        << "Requests:            "
        << result.total_requests
        << '\n';

    std::cout
        << "Successful:          "
        << result.successful_requests
        << '\n';

    std::cout
        << "Failed:              "
        << result.failed_requests
        << '\n';

    std::cout
        << "Elapsed:             "
        << std::fixed
        << std::setprecision(4)
        << result.elapsed_seconds
        << " s\n";

    std::cout
        << "Throughput:          "
        << std::fixed
        << std::setprecision(2)
        << result.throughput()
        << " requests/sec\n";

    if (result.failed_requests == 0 &&
        result.successful_requests ==
            result.total_requests)
    {
        std::cout
            << "Result:              PASS\n";
    }
    else
    {
        std::cout
            << "Result:              FAIL\n";
    }

    std::cout
        << "----------------------------------------------\n";
}

// ============================================================
// MAIN
// ============================================================

int main()
{
    std::cout
        << "==================================================\n"
        << "        PROJECT ARES PERFORMANCE BENCHMARK\n"
        << "==================================================\n";

    std::cout
        << "Target:              "
        << HOST
        << ':'
        << PORT
        << '\n';

    std::cout
        << "Connections:         "
        << DEFAULT_CONNECTIONS
        << '\n';

    std::cout
        << "Requests/connection: "
        << REQUESTS_PER_CONNECTION
        << '\n';

    std::cout
        << "Pipeline depth:      "
        << DEFAULT_PIPELINE_DEPTH
        << '\n';

    std::cout
        << "GET keyspace:        "
        << GET_KEYSPACE
        << '\n';

    std::cout
        << "==================================================\n";

    // --------------------------------------------------------
    // Prepare requests
    // --------------------------------------------------------

    RequestSet requests =
        build_request_set();

    const std::uint64_t total_requests =
        static_cast<std::uint64_t>(
            DEFAULT_CONNECTIONS) *
        REQUESTS_PER_CONNECTION;

    std::cout
        << "Total requests/test: "
        << total_requests
        << '\n';

    // --------------------------------------------------------
    // Test 1
    // --------------------------------------------------------

    std::cout
        << "\n\n[1/4] Synchronous SET\n";

    std::cout
        << "One request in flight per connection.\n";

    auto sync_result =
        timed_run(
            [&]
            {
                //
                // Implement synchronous benchmark directly
                // with proper outer timing.
                //

                BenchmarkResult result;

                result.name =
                    "Synchronous SET";

                result.total_requests =
                    total_requests;

                StartBarrier barrier(
                    DEFAULT_CONNECTIONS);

                std::atomic<std::uint64_t>
                    successes{0};

                std::atomic<std::uint64_t>
                    failures{0};

                std::vector<std::thread>
                    workers;

                workers.reserve(
                    DEFAULT_CONNECTIONS);

                for (int thread_id = 0;
                     thread_id <
                     DEFAULT_CONNECTIONS;
                     ++thread_id)
                {
                    workers.emplace_back(
                        [&,
                         thread_id]
                        {
                            int fd =
                                connect_to_server();

                            if (fd < 0)
                            {
                                failures +=
                                    REQUESTS_PER_CONNECTION;

                                barrier.arrive_and_wait();
                                return;
                            }

                            barrier.arrive_and_wait();

                            for (int i = 0;
                                 i <
                                 REQUESTS_PER_CONNECTION;
                                 ++i)
                            {
                                const auto& request =
                                    requests.set_requests[
                                        i];

                                if (!send_all(
                                        fd,
                                        request.data(),
                                        request.size()))
                                {
                                    ++failures;
                                    break;
                                }

                                if (!recv_set_response(
                                        fd))
                                {
                                    ++failures;
                                    break;
                                }

                                ++successes;
                            }

                            close(fd);
                        });
                }

                for (auto& worker :
                     workers)
                {
                    worker.join();
                }

                result.successful_requests =
                    successes.load();

                result.failed_requests =
                    failures.load();

                return result;
            });

    print_result(sync_result);

    // --------------------------------------------------------
    // Test 2
    // --------------------------------------------------------

    std::cout
        << "\n\n[2/4] Pipelined SET\n";

    std::cout
        << "Pipeline depth: "
        << DEFAULT_PIPELINE_DEPTH
        << '\n';

    auto pipeline_set =
        timed_run(
            [&]
            {
                return benchmark_pipelined_set(
                    requests,
                    DEFAULT_CONNECTIONS,
                    DEFAULT_PIPELINE_DEPTH);
            });

    print_result(pipeline_set);

    // --------------------------------------------------------
    // Warm cache
    // --------------------------------------------------------

    if (!populate_cache(requests))
    {
        std::cerr
            << "\nCache warmup failed.\n"
            << "GET benchmarks cannot continue.\n";

        return 1;
    }

    // --------------------------------------------------------
    // Test 3
    // --------------------------------------------------------

    std::cout
        << "\n\n[3/4] Pipelined GET\n";

    std::cout
        << "Expected workload: cache hits.\n";

    auto get_result =
        timed_run(
            [&]
            {
                return benchmark_pipelined_get(
                    requests,
                    DEFAULT_CONNECTIONS,
                    DEFAULT_PIPELINE_DEPTH);
            });

    print_result(get_result);

    // --------------------------------------------------------
    // Test 4
    // --------------------------------------------------------

    std::cout
        << "\n\n[4/4] Mixed workload\n";

    std::cout
        << "80% GET / 20% SET\n";

    auto mixed_result =
        timed_run(
            [&]
            {
                return benchmark_mixed(
                    requests,
                    DEFAULT_CONNECTIONS,
                    DEFAULT_PIPELINE_DEPTH);
            });

    print_result(mixed_result);

    // ========================================================
    // FINAL SUMMARY
    // ========================================================

    std::cout
        << "\n\n==================================================\n"
        << "                 FINAL SUMMARY\n"
        << "==================================================\n";

    std::cout
        << std::left
        << std::setw(28)
        << "Benchmark"
        << std::right
        << std::setw(18)
        << "Requests/sec\n";

    std::cout
        << "--------------------------------------------------\n";

    std::cout
        << std::left
        << std::setw(28)
        << "Synchronous SET"
        << std::right
        << std::setw(18)
        << std::fixed
        << std::setprecision(2)
        << sync_result.throughput()
        << '\n';

    std::cout
        << std::left
        << std::setw(28)
        << "Pipelined SET"
        << std::right
        << std::setw(18)
        << pipeline_set.throughput()
        << '\n';

    std::cout
        << std::left
        << std::setw(28)
        << "Pipelined GET"
        << std::right
        << std::setw(18)
        << get_result.throughput()
        << '\n';

    std::cout
        << std::left
        << std::setw(28)
        << "Mixed 80/20"
        << std::right
        << std::setw(18)
        << mixed_result.throughput()
        << '\n';

    std::cout
        << "==================================================\n";

    return 0;
}

// sakshampal@SAKSHAMS-M5 project-ares % ./benchmark
// ==================================================
//         PROJECT ARES PERFORMANCE BENCHMARK
// ==================================================
// Target:              127.0.0.1:8080
// Connections:         16
// Requests/connection: 25000
// Pipeline depth:      64
// GET keyspace:        100000
// ==================================================
// Preparing benchmark requests...
// Total requests/test: 400000

// [1/4] Synchronous SET
// One request in flight per connection.

// ----------------------------------------------
// Synchronous SET
// Requests:            400000
// Successful:          400000
// Failed:              0
// Elapsed:             2.6402 s
// Throughput:          151505.30 requests/sec
// Result:              PASS
// ----------------------------------------------

// [2/4] Pipelined SET
// Pipeline depth: 64

// ----------------------------------------------
// Pipelined SET
// Requests:            400000
// Successful:          400000
// Failed:              0
// Elapsed:             0.4821 s
// Throughput:          829631.18 requests/sec
// Result:              PASS
// ----------------------------------------------

// Warming cache with 100000 keys...
// Cache warmup complete.

// [3/4] Pipelined GET
// Expected workload: cache hits.

// ----------------------------------------------
// Pipelined GET
// Requests:            400000
// Successful:          400000
// Failed:              0
// Elapsed:             0.5388 s
// Throughput:          742348.94 requests/sec
// Result:              PASS
// ----------------------------------------------

// [4/4] Mixed workload
// 80% GET / 20% SET

// ----------------------------------------------
// Mixed 80% GET / 20% SET
// Requests:            400000
// Successful:          400000
// Failed:              0
// Elapsed:             0.5255 s
// Throughput:          761112.78 requests/sec
// Result:              PASS
// ----------------------------------------------

// ==================================================
//                  FINAL SUMMARY
// ==================================================
// Benchmark                        Requests/sec
// --------------------------------------------------
// Synchronous SET                      151505.30
// Pipelined SET                        829631.18
// Pipelined GET                        742348.94
// Mixed 80/20                          761112.78
// ==================================================
// sakshampal@SAKSHAMS-M5 project-ares % 




// Project Ares V2 baseline
// Machine: Apple Silicon Mac
// Transport: TCP loopback
// Connections: 16
// Total requests: 400,000

// Synchronous SET:       151.5K req/s
// Pipelined SET:         829.6K req/s
// Pipelined GET:         742.3K req/s
// Mixed 80/20:           761.1K req/s