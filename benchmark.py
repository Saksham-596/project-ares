# script given by chatGPT
import socket
import time
import threading
import struct

# Configuration
NUM_THREADS = 50
REQUESTS_PER_THREAD = 4000  # 50 * 4000 = 200,000 total unique requests
TOTAL_REQUESTS = NUM_THREADS * REQUESTS_PER_THREAD

successful_requests = 0
counter_lock = threading.Lock()

print(f"Pre-computing {TOTAL_REQUESTS} unique binary payloads...")
print("This guarantees Python is not the bottleneck. Stand by...")

payloads = []
for i in range(TOTAL_REQUESTS):
    key = f"key_{i}".encode('utf-8')
    val = f"val_data_{i}".encode('utf-8')
    header = struct.pack(">B I I", 1, len(key), len(val))
    payloads.append(header + key + val)

def benchmark_worker(thread_index):
    global successful_requests
    local_success = 0
    start_idx = thread_index * REQUESTS_PER_THREAD
    end_idx = start_idx + REQUESTS_PER_THREAD
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 8080))
        
        for i in range(start_idx, end_idx):
            s.sendall(payloads[i])
            response = s.recv(1024) 
            if response == b'OK\n':
                local_success += 1
                
        s.close()
    except Exception as e:
        pass
        
    with counter_lock:
        successful_requests += local_success

print("Payloads loaded. Engaging Project Ares Engine...")
print("-" * 50)
start_time = time.time()
threads = []

for i in range(NUM_THREADS):
    t = threading.Thread(target=benchmark_worker, args=(i,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

end_time = time.time()
time_taken = end_time - start_time

print(f"Chaos Benchmark Complete in {time_taken:.4f} seconds.")
print(f"Total Unique Keys Injected: {successful_requests}")

if time_taken > 0 and successful_requests > 0:
    print(f"True Production Throughput: {successful_requests / time_taken:.2f} Requests/Second.")
else:
    print("True Production Throughput: 0 Requests/Second.")
print("-" * 50)
'''
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7045 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 117336.81 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7538 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 114035.88 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7542 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 114011.17 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7255 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 115909.71 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7338 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 115352.31 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7470 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 114480.53 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7349 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 115279.44 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.7463 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 114527.13 Requests/Second.
--------------------------------------------------
'''