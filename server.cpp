#include<iostream>
#include<sys/socket.h>
#include<netinet/in.h>
#include<unistd.h>
#include<unordered_map>
#include<sstream>
#include<thread>
#include<mutex>
#include<chrono>
#include<vector>
#include<queue>
#include<condition_variable>
#include<cstring>
#include<arpa/inet.h>
#include<atomic>
#include<csignal>

// memory architecture
const int NUM_SHARDS = 16;

// doubly linked list
struct Node {
    std::string key , value;
    Node* prev ;
    Node* next ;
    Node(std::string k , std::string v) : key(k) , value(v) , prev(nullptr) , next(nullptr) {}
};

// The LRU shard
struct Shard {
    int capacity = 10000 ; // 10k per shard , total 160k capacity
    std::unordered_map<std::string ,Node*> map;
    Node* head; // MRU ( most recently used)
    Node* tail; // LRU (least recently used)
    std::mutex lock;
    Shard() {
        // dummy head and tail to prevent accessing nullptr
        head = new Node("","");
        tail = new Node("","");
        head->next = tail;
        tail->prev = head;
    }
    // Helper : snip node out of the list
    void removeNode(Node * node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }
    // Helper : Attach node at the right behind the Head (MRU)
    void addHead(Node * node) {
        node->next = head->next;
        node->prev = head;
        head->next->prev  = node;
        head->next = node;
    }
};
// array of 16 independent LRU shards

std::vector<Shard> database_shard(NUM_SHARDS);

// thread pool state
std::queue<int> task_queue;
std::mutex queue_mutex;
std::condition_variable cv;

// shutdown architecture
std::atomic<bool> keep_running{true};
int server_fd = -1;

void signal_handler(int signum) {
    std::cout << "\n[Server] SIGINT recieved , intializing shutdown ... " << std::endl;
    keep_running = false;
    if(server_fd>=0){
        shutdown(server_fd,SHUT_RDWR);
    }
    cv.notify_all(); // wake up all sleeping worker thread instantly
}

void worker_thread(int worker_id) {
    std::cout << "[Worker " << worker_id << "] online and standing by..." << std::endl;
    
    while(keep_running) {
        int client_socket;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            cv.wait(lock,[]{return!task_queue.empty() || !keep_running;});
            if(!keep_running && task_queue.empty()) {
                break;
            }
            client_socket = task_queue.front();
            task_queue.pop();
        }
        struct timeval tv;
        tv.tv_sec = 30;
        tv.tv_usec = 0;
        setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO,(const char*)&tv ,sizeof(tv));
        
        std::vector<uint8_t> buffer;
        while(true) {
            char temp_buffer[1024] = {0};
            ssize_t byte_read  = read(client_socket,temp_buffer,1024);
            if(byte_read <=0)break;
            buffer.insert(buffer.end(),temp_buffer,temp_buffer+byte_read);
            
            while(buffer.size()>=9) {
                uint8_t opcode = buffer[0];
                
                uint32_t val_len , key_len;
                std::memcpy(&key_len,buffer.data() + 1 , 4) ;
                key_len = ntohl(key_len);
                std::memcpy(&val_len , buffer.data() + 5 , 4) ;
                val_len = ntohl(val_len);
                
                uint32_t total_frame_size = 9 + key_len + val_len;
                if(buffer.size() < total_frame_size) break;
                std::string key((char*)buffer.data() + 9 , key_len);
                std::string value((char*)buffer.data() + 9 + key_len,val_len);
                buffer.erase(buffer.begin(), buffer.begin() + total_frame_size);
                
                size_t shard_index = std::hash<std::string>{}(key)%NUM_SHARDS;
                const char* response = "ERR\n";
                int response_len = 4 ;
                {
                    std::lock_guard<std::mutex> db_lock(database_shard[shard_index].lock);
                    Shard& shard = database_shard[shard_index];
                    
                    
                    if(opcode == 0x01){// setcommand;
                        if(shard.map.count(key)>0) {
                            Node * node = shard.map[key];
                            node->value = value;
                            shard.removeNode(node);
                            shard.addHead(node);
                        }else {
                            Node * newNode = new Node(key,value);
                            shard.map[key] = newNode;
                            shard.addHead(newNode);
                            // evict the LRU element if over capacity
                            if(shard.map.size() > shard.capacity) {
                                Node* lru = shard.tail->prev;
                                shard.removeNode(lru);
                                shard.map.erase(lru->key);
                                delete lru;
                            }
                        }
                        response = "OK\n";
                        response_len = 3;
                    }else if(opcode == 0x02) { // GET command
                        if(shard.map.count(key) > 0 ){
                            Node * node = shard.map[key];
                            shard.removeNode(node);
                            shard.addHead(node);
                            response = "OK\n";
                            response_len = 3;
                        }else{
                            response = "nil\n";
                            response_len = 4;
                        }
                    }
                }
                send(client_socket,response,response_len,0);
            }
        }
        close(client_socket);
    }
}

int main() {
    // Arm the kill switch
    std::signal(SIGINT,signal_handler);
    // Arm the kill switch with strict POSIX sigaction
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; // The magic line: 0 explicitly disables SA_RESTART
    sigaction(SIGINT, &sa, nullptr);
    
    // open the tcp socket
    server_fd = socket(AF_INET , SOCK_STREAM,0);
    if(server_fd < 0 ) {
        std::cerr << "[Fatal error] failed to create socket" << std::endl;
        return 1;
    }
    // set SO_REUSEADDR to prevent "port already in use" error if restarted quickly
    int opt = 1 ;
    setsockopt(server_fd , SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
    
    struct sockaddr_in  address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);
    if(bind(server_fd ,(struct sockaddr*)&address,sizeof(address))<0) {
        std::cerr <<"[Fatal error] Failed to bind to port 8080." << std::endl;
        return 1;
    }
    listen(server_fd,1000);
    std::cout << "Project Ares engine online... port: 8000" << std::endl;
    std::cout << "Architecture: 16-way LRU cache. capacity 160k keys." << std::endl;
    // Deploy threads
    std::vector<std::thread> workers;
    for(int i = 0;i<3;i++) {
        workers.emplace_back(worker_thread,i);
    }
    while(keep_running) {
        int client_socket = accept(server_fd, nullptr,nullptr);
        // if accept is interrupted by SIGINT singnal
        if(client_socket < 0 ) {
            if(!keep_running)break;
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            task_queue.push(client_socket);
        }
        cv.notify_one();
    }
    // clean up
    std::cout << "\n[Server] sealing port 8080..." <<std::endl;
    close(server_fd);
    std::cout << "\n[Server] Awaiting thread pool convergence..." << std::endl;
    for(auto & worker : workers) {
        if(worker.joinable()) {
            worker.join();
        }
    }
    std:: cout << "[Server] Engine offline. All memory released safely. " << std::endl;
    return 0;
}

/*
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
----------------------------------------------------------------------------------------------------
Chaos Benchmark Complete in 1.6033 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 124743.90 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.5822 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 126409.94 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.5846 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 126214.19 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.5763 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 126879.91 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.5989 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 125085.36 Requests/Second.
--------------------------------------------------
sakshampal@SAKSHAMS-M5 project-ares % python3 benchmark.py
Pre-computing 200000 unique binary payloads...
This guarantees Python is not the bottleneck. Stand by...
Payloads loaded. Engaging Project Ares Engine...
--------------------------------------------------
Chaos Benchmark Complete in 1.5763 seconds.
Total Unique Keys Injected: 200000
True Production Throughput: 126882.06 Requests/Second.*/