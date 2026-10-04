import socket
import time
import threading
import struct

# ============================================================
# PROJECT ARES V2 BENCHMARK
# ============================================================

HOST = "127.0.0.1"
PORT = 8080

NUM_THREADS = 50
REQUESTS_PER_THREAD = 4000

TOTAL_REQUESTS = NUM_THREADS * REQUESTS_PER_THREAD

# ============================================================
# RESULT TRACKING
# ============================================================

successful_requests = 0
failed_requests = 0

counter_lock = threading.Lock()


# ============================================================
# PRE-COMPUTE REQUESTS
# ============================================================
#
# Protocol:
#
# byte 0      : opcode
# bytes 1-4   : key length (network byte order)
# bytes 5-8   : value length (network byte order)
# bytes 9...  : key + value
#
# SET opcode = 0x01
#
# V2 uses the exact same protocol as V1.
# This benchmark therefore isolates the server-side
# implementation changes.
# ============================================================

print(f"Preparing {TOTAL_REQUESTS} requests...")

payloads = []

for i in range(TOTAL_REQUESTS):

    key = f"key_{i}".encode("utf-8")
    value = f"val_data_{i}".encode("utf-8")

    payload = (
        struct.pack(
            ">BII",
            0x01,              # SET
            len(key),
            len(value)
        )
        + key
        + value
    )

    payloads.append(payload)


# ============================================================
# RECEIVE EXACT RESPONSE
# ============================================================
#
# SET response is:
#
#     OK\n
#
# exactly 3 bytes.
#
# TCP is a byte stream, so recv(3) is not theoretically
# guaranteed to return all 3 bytes.
#
# recv_exact() makes the benchmark itself TCP-correct.
# ============================================================

def recv_exact(sock, n):

    data = bytearray()

    while len(data) < n:

        chunk = sock.recv(n - len(data))

        if not chunk:
            return None

        data.extend(chunk)

    return bytes(data)


# ============================================================
# BENCHMARK WORKER
# ============================================================

def benchmark_worker(thread_index):

    global successful_requests
    global failed_requests

    local_success = 0
    local_failed = 0

    start_index = (
        thread_index *
        REQUESTS_PER_THREAD
    )

    end_index = (
        start_index +
        REQUESTS_PER_THREAD
    )

    sock = None

    try:

        # ----------------------------------------------------
        # Create one persistent TCP connection per benchmark
        # thread.
        # ----------------------------------------------------

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        sock.connect(
            (HOST, PORT)
        )

        # ----------------------------------------------------
        # Send requests sequentially on this connection.
        #
        # Multiple benchmark threads provide concurrent
        # clients while each client preserves request/response
        # ordering.
        # ----------------------------------------------------

        for i in range(
            start_index,
            end_index
        ):

            sock.sendall(
                payloads[i]
            )

            response = recv_exact(
                sock,
                3
            )

            if response == b"OK\n":
                local_success += 1

            else:
                local_failed += 1

    except Exception as e:

        local_failed += (
            end_index -
            start_index -
            local_success -
            local_failed
        )

        print(
            f"[Worker {thread_index}] "
            f"failed: {e}"
        )

    finally:

        if sock is not None:

            try:
                sock.close()

            except Exception:
                pass

    # --------------------------------------------------------
    # Aggregate results.
    # --------------------------------------------------------

    with counter_lock:

        successful_requests += (
            local_success
        )

        failed_requests += (
            local_failed
        )


# ============================================================
# BENCHMARK INFORMATION
# ============================================================

print()
print("==============================================")
print("       PROJECT ARES V2 BENCHMARK")
print("==============================================")
print(f"Threads:           {NUM_THREADS}")
print(f"Requests/thread:   {REQUESTS_PER_THREAD}")
print(f"Total requests:    {TOTAL_REQUESTS}")
print(f"Target:            {HOST}:{PORT}")
print("Protocol:          Binary TCP")
print("Operation:         SET")
print("==============================================")
print()


# ============================================================
# START BENCHMARK
# ============================================================

print("Benchmark starting...")

start_time = time.perf_counter()

threads = []


for i in range(NUM_THREADS):

    thread = threading.Thread(
        target=benchmark_worker,
        args=(i,)
    )

    thread.start()

    threads.append(thread)


# ============================================================
# WAIT FOR ALL CLIENTS
# ============================================================

for thread in threads:

    thread.join()


# ============================================================
# RESULTS
# ============================================================

elapsed = (
    time.perf_counter() -
    start_time
)


throughput = (
    successful_requests /
    elapsed
    if elapsed > 0
    else 0
)


print()
print("----------------------------------------------")
print("Benchmark complete")
print("----------------------------------------------")

print(
    f"Successful requests: {successful_requests}"
)

print(
    f"Failed requests:     {failed_requests}"
)

print(
    f"Elapsed time:        {elapsed:.4f}s"
)

print(
    f"Throughput:          {throughput:.2f} requests/sec"
)

print("----------------------------------------------")

if successful_requests == TOTAL_REQUESTS:

    print(
        "RESULT: PASS - all requests succeeded"
    )

else:

    print(
        "RESULT: FAIL - some requests failed"
    )

print("----------------------------------------------")