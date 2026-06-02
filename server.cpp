#include<iostream>
#include<sys/socket.h> // it will open the port or socket
#include<netinet/in.h> // header of IP address
#include<unistd.h> // header to close the port or socket
#include<unordered_map>
#include<sstream>
#include<thread>
#include<mutex>
#include<chrono>
#include<vector>
#include<queue>
#include<condition_variable>
#include <cstring>      // Required for std::memcpy
#include <arpa/inet.h>  // Required for ntohl()

// THE sharded database to solve the lock contention problem by db_mutex
const int NUM_SHARDS = 16;
struct Shard {
    std::unordered_map<std::string,std::string> map;
    std::mutex lock;
};

// array of 16 independent shards 
std::vector<Shard> database_shards(NUM_SHARDS);
// sweeper thread logic
std::queue<int> task_queue;
std::mutex queue_mutex;
std::condition_variable cv;

void worker_thread(int worker_id) {
    std::cout << "[Worker " << worker_id << "] Online and standing by." << std::endl;
    while(true) {
        int client_socket;
        {
            // wait for ticket 
            std::unique_lock<std::mutex> lock(queue_mutex);
            cv.wait(lock,[]{ return !task_queue.empty();});
            client_socket = task_queue.front();
            task_queue.pop();
        }
        
        // 30 second time out 
        struct timeval tv ;
        tv.tv_sec = 30;
        tv.tv_usec = 0;
        setsockopt(client_socket,SOL_SOCKET , SO_RCVTIMEO, (const char *)&tv , sizeof(tv));
        // fixes TCP fragmentation 
        std::vector<uint8_t> buffer;
        while(true) {
            char temp_buffer[1024] = {0};
            ssize_t byte_read = read(client_socket, temp_buffer,1024);
            if(byte_read <= 0) { // client disconnected or timeout
                break;
            }
            // append incoming data to accumulator -> buffer 
            buffer.insert(buffer.end(),temp_buffer, temp_buffer+byte_read);
            // process everything now 
            while (buffer.size() >= 9) { // 9 byte is the minimum size of header
                // read the header 
                uint8_t opcode = buffer[0];
                // Read key length ,  network byte order to host byte order
                uint32_t val_len , key_len;
                std::memcpy (&key_len,buffer.data() + 1, 4);
                key_len = ntohl(key_len);
                // read value length
                std::memcpy (&val_len,buffer.data() + 5 , 4);
                val_len = ntohl(val_len);
                // calculate the exact size of this specific command
                uint32_t total_frame_size = 9 + key_len + val_len;
                // if TCP chopped the packet and we don't have the full frame yet 
                // break it and wait for more data.
                if(buffer.size() < total_frame_size) {
                    break;
                }
                //Extract Data 
                std::string key((char*)buffer.data() + 9 , key_len);
                std::string value((char*)buffer.data() + 9 + key_len,val_len);
                // Erase this frame from the buffer so we can process the next one 
                buffer.erase(buffer.begin() , buffer.begin() + total_frame_size);
                // Sharded database Execution 
                size_t shard_index = std::hash<std::string>{}(key)%NUM_SHARDS;
                const char* response = "ERR\n";
                int response_len = 4;
                {
                    std::lock_guard<std::mutex> db_lock(database_shards[shard_index].lock);

                    if(opcode == 0x01) { // SET command 
                        database_shards[shard_index].map[key] = value;
                        response = "OK\n";
                        response_len = 3;
                    }else if(opcode == 0x02) { // GET command
                        if(database_shards[shard_index].map.count(key) > 0) {
                            response = "OK\n";
                            response_len =3;
                        }else {
                            response = "nil\n";
                            response_len = 4;

                        }

                    }
                } // mutex unlocks here 
                // send response 
                send(client_socket,response,response_len,0);
            }
        }
        close(client_socket);
    }
}

void memory_sweeper(){
     while(true) { 
        // sleep for 10 seconds;
        std::this_thread::sleep_for(std::chrono::seconds(10));
        // loop all 16 shards and wipe them 
        for(int i = 0;i<NUM_SHARDS;i++) {
            std::lock_guard<std::mutex> lock(database_shards[i].lock);
            if(!database_shards[i].map.empty()) {
                database_shards[i].map.clear();
            }
        }
        std::cout << "\n[Sweeper] all 16 shards are cleaned." << std::endl;
     }
}

int main(){
    int server_fd = socket(AF_INET,SOCK_STREAM,0);
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);
    bind(server_fd,(struct sockaddr*)&address , sizeof(address));
    listen(server_fd,1000);
    std::cout << "Project Ares Engine Online , 4 Worker threads are deployed" << std::endl;
    // spin up the thread pool
    std::thread sweeper(memory_sweeper);
        sweeper.detach();
    for(int i = 0;i<4;i++) {
        std::thread worker(worker_thread,i);
        worker.detach();
    }
    // Main thread takes the request
    while(true) {
        int client_socket = accept(server_fd,nullptr,nullptr);
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            task_queue.push(client_socket);
        }
        // notify exactly one sleeping thread to handle it 
        cv.notify_one(); 
    }
    return 0;
}