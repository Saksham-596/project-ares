# Project Ares ⚡

**A High-Performance, Multithreaded In-Memory Database built in pure C++17.**

Project Ares is a custom key-value store engineered from the ground up using raw POSIX sockets. Designed for extreme throughput and low latency, it bypasses standard string-parsing overhead by implementing a custom zero-copy binary protocol, achieving over **82,000 concurrent operations per second** on standard consumer hardware(apple macbook m1 air).

---

## 🧠 Architecture & Core Features

* **Zero-Copy Binary Protocol:** Eliminated `std::string` parsing overhead by designing a custom length-prefixed binary application protocol. Operations use direct pointer arithmetic (`memcpy`, `ntohl`) for O(1) memory extraction.
* **16-Way Mutex Sharding:** Solved CPU lock contention by splitting the global database map into 16 independent, hashed memory shards, allowing true parallel writes across multi-core processors.
* **Custom Thread Pool:** Built a lock-free ticket queue using `std::mutex` and `std::condition_variable` to eliminate OS-level thread-creation overhead and handle massive concurrent client loads.
* **TCP Frame Decoder:** Engineered an accumulator buffer to safely reconstruct fragmented TCP streams and prevent protocol corruption under heavy network traffic.
* **DDoS & Resource Protection:** Implemented persistent TCP Keep-Alive connections and OS-level socket timeouts (`setsockopt`) to eliminate handshake bottlenecks and defend against Slowloris resource-exhaustion attacks.

---

## 📊 Benchmarks

Throughput was verified using a custom Python multi-threading benchmark script firing 60,000 concurrent network payloads.

* **Throughput:** `82,213 Requests/Second`
* **Execution Time (60k payloads):** `0.7298 Seconds`
* **Environment:** macOS (Apple Silicon / Unified Memory), Localhost Loopback.

---

## 🛠️ Tech Stack

* **Language:** C++17
* **Networking:** Raw POSIX Sockets (`<sys/socket.h>`, `<arpa/inet.h>`)
* **Concurrency:** `std::thread`, `std::mutex`, `std::condition_variable`
* **Benchmarking:** Python 3 (`struct`, `socket`, `threading`)

---

## 🔌 Protocol Specification (Binary Header)

Ares drops text-based communication (like `SET Key Value`) in favor of a strict 9-byte binary frame to prevent packet coalescing and fragmentation.

| Byte Offset | Size | Description |
| :--- | :--- | :--- |
| `0` | 1 Byte (`uint8_t`) | Opcode (`0x01` = SET, `0x02` = GET, `0x03` = DEL) |
| `1-4` | 4 Bytes (`uint32_t`) | Key Length (Network Byte Order) |
| `5-8` | 4 Bytes (`uint32_t`) | Value Length (Network Byte Order) |
| `9+` | Variable | Raw Key Bytes + Raw Value Bytes |

---

## 🚀 Getting Started

### 1. Build the Server
Requires a C++17 compatible compiler (`clang++` or `g++`).

```bash
git clone [https://github.com/Saksham-596/project-ares.git](https://github.com/Saksham-596/project-ares.git)
cd project-ares
```
## 2. Run the Engine
```bash
clang++ -std=c++17 -O3 -Wall -Wextra server.cpp -o ares_server
./ares_server
 #Output: Project Ares Engine Online. Binary Protocol Active.
 ```
 ## 3. Run the Benchmark
In a separate terminal instance, fire the binary payload script:
```bash
python3 benchmark.py
```
## 🔮 Future Roadmap
* While currently optimized for pure throughput, future iterations will focus on production durability:

* Write-Ahead Log (WAL): Appending raw binary opcodes to disk for crash recovery and persistence.

* Event-Driven I/O: Migrating from thread-per-connection to epoll (Linux) / kqueue (macOS) to solve the C10K connection problem.

* Eviction Policies: Implementing an LRU (Least Recently Used) cache mechanism to gracefully handle maximum memory limits.
***
### How to use this:
1. Create a new repository on your GitHub.
2. Add your `server.cpp` and `benchmark.py` files.
3. Paste this exact Markdown into the `README.md`. 
4. Replace `yourusername` in the clone link with your actual GitHub handle.

When an interviewer or recruiter clicks the GitHub link on your resume, they won't just see a wall of code. They will see the binary spec table, the architecture decisions, and the benchmark right at the top. It frames the project perfectly before they even look at a single `for` loop.

## Developed by Saksham.

