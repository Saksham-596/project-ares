# generating from  chatGPT
import socket
import threading
import time
import struct

# --- CONFIGURATION ---
TARGET_HOST = '127.0.0.1'
TARGET_PORT = 8080
NUM_THREADS = 100        
REQUESTS_PER_THREAD = 600 # 100 clients x 600 commands each 
# Total Requests = 60,000

start_barrier = threading.Barrier(NUM_THREADS)

def worker_client(thread_id):
    start_barrier.wait()

    # 1. DIAL THE PHONE ONCE
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((TARGET_HOST, TARGET_PORT))
    except:
        return

    # 2. FIRE BINARY PAYLOADS
    for i in range(REQUESTS_PER_THREAD):
        key = f"Key_{thread_id}_{i}".encode('utf-8')
        value = f"{i * 10}".encode('utf-8')
        
        # PACK THE RAW BYTES
        # '!B I I' means:
        # ! = Network Endian (Big-Endian)
        # B = 1 Byte Unsigned Char (Opcode 1 = SET)
        # I = 4 Byte Unsigned Int (Key Length)
        # I = 4 Byte Unsigned Int (Value Length)
        header = struct.pack('!B I I', 1, len(key), len(value))
        
        payload = header + key + value
        
        # sendall() is safer for raw binary than send()
        client.sendall(payload)
        
        try:
            # We expect a 3-byte response ("OK\n")
            client.recv(3) 
        except:
            break
            
    client.close()

# --- THE EXECUTION ---
print(f"Deploying {NUM_THREADS} concurrent threads...")
threads = []
for i in range(NUM_THREADS):
    t = threading.Thread(target=worker_client, args=(i,))
    threads.append(t)
    t.start()

start_time = time.time()
for t in threads:
    t.join()
end_time = time.time()

duration = end_time - start_time
total_requests = NUM_THREADS * REQUESTS_PER_THREAD
requests_per_second = total_requests / duration

print("--------------------------------------------------")
print(f"Benchmark Complete in {duration:.4f} seconds.")
print(f"Throughput: {requests_per_second:.2f} Requests/Second.")
print("--------------------------------------------------")