import socket
import time
import threading
import struct

HOST = "127.0.0.1"
PORT = 8080

NUM_THREADS = 50
REQUESTS_PER_THREAD = 4000
TOTAL_REQUESTS = NUM_THREADS * REQUESTS_PER_THREAD

successful_requests = 0
counter_lock = threading.Lock()

print(f"Preparing {TOTAL_REQUESTS} requests...")

payloads = []

for i in range(TOTAL_REQUESTS):
    key = f"key_{i}".encode()
    value = f"val_data_{i}".encode()

    payloads.append(
        struct.pack(
            ">BII",
            1,
            len(key),
            len(value)
        )
        + key
        + value
    )


def recv_exact(sock, n):
    data = bytearray()

    while len(data) < n:
        chunk = sock.recv(n - len(data))

        if not chunk:
            return None

        data.extend(chunk)

    return bytes(data)


def benchmark_worker(thread_index):
    global successful_requests

    local_success = 0

    start = thread_index * REQUESTS_PER_THREAD
    end = start + REQUESTS_PER_THREAD

    try:
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        sock.connect((HOST, PORT))

        for i in range(start, end):

            sock.sendall(payloads[i])

            response = recv_exact(sock, 3)

            if response == b"OK\n":
                local_success += 1

        sock.close()

    except Exception as e:
        print(f"Worker {thread_index} failed: {e}")

    with counter_lock:
        successful_requests += local_success


print("Benchmark starting...")
print(f"Threads: {NUM_THREADS}")
print(f"Requests: {TOTAL_REQUESTS}")
print(f"Target: {HOST}:{PORT}")

start_time = time.perf_counter()

threads = []

for i in range(NUM_THREADS):
    thread = threading.Thread(
        target=benchmark_worker,
        args=(i,)
    )

    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

elapsed = time.perf_counter() - start_time

print("-" * 50)
print(f"Successful requests: {successful_requests}")
print(f"Elapsed time: {elapsed:.4f}s")

if elapsed > 0:
    throughput = successful_requests / elapsed
    print(f"Throughput: {throughput:.2f} requests/sec")

print("-" * 50)
#  sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
# Preparing 200000 requests...
# Benchmark starting...
# Threads: 50
# Requests: 200000
# Target: 127.0.0.1:8080
# --------------------------------------------------
# Successful requests: 200000
# Elapsed time: 1.7472s 
# Throughput: 114465.85 requests/sec
# --------------------------------------------------
# sakshampal@SAKSHAMS-M5 project-ares % 