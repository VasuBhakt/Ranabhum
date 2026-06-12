#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// Simple helper to extract string values from JSON
std::string get_json_string(const std::string& json, const std::string& key) {
    size_t pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) return "";
    
    pos = json.find(":", pos);
    if (pos == std::string::npos) return "";
    
    size_t start = json.find("\"", pos);
    if (start == std::string::npos) return "";
    start++; 
    
    size_t end = json.find("\"", start);
    if (end == std::string::npos) return "";
    
    return json.substr(start, end - start);
}

// Simple helper to extract numeric values from JSON
double get_json_numeric(const std::string& json, const std::string& key) {
    size_t pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) return 0.0;
    
    pos = json.find(":", pos);
    if (pos == std::string::npos) return 0.0;
    pos++; 
    
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
        pos++;
    }
    
    std::string val;
    while (pos < json.size() && ((json[pos] >= '0' && json[pos] <= '9') || json[pos] == '.' || json[pos] == '-')) {
        val += json[pos];
        pos++;
    }
    
    if (val.empty()) return 0.0;
    return std::stod(val);
}

struct BookOrder {
    std::string order_id;
    std::string side;
    double price;
    int quantity;
    long long timestamp;
};

struct MatchResult {
    int actual_fill_qty = 0;
    double actual_fill_price = 0.0;
    std::string status = "ack";
    std::vector<std::string> matched_order_ids;
};

// Thread-safe in-memory order book simulating real price-time priority matching
class OrderBook {
private:
    std::vector<BookOrder> bids; // sorted highest price first
    std::vector<BookOrder> asks; // sorted lowest price first
    std::mutex book_mutex;

public:
    MatchResult process_order(const std::string& order_id, const std::string& order_type, const std::string& side, double price, int quantity) {
        std::lock_guard<std::mutex> lock(book_mutex);
        MatchResult res;

        if (order_type == "cancel") {
            bool found = false;
            for (auto it = bids.begin(); it != bids.end(); ++it) {
                if (it->order_id == order_id) {
                    bids.erase(it);
                    found = true;
                    break;
                }
            }
            if (!found) {
                for (auto it = asks.begin(); it != asks.end(); ++it) {
                    if (it->order_id == order_id) {
                        asks.erase(it);
                        found = true;
                        break;
                    }
                }
            }
            res.status = found ? "cancelled" : "rejected";
            return res;
        }

        long long now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();

        BookOrder new_order{order_id, side, price, quantity, now_ns};

        if (side == "buy") {
            // Match against asks (sellers)
            std::sort(asks.begin(), asks.end(), [](const BookOrder& a, const BookOrder& b) {
                if (a.price != b.price) return a.price < b.price; // cheapest first
                return a.timestamp < b.timestamp;                 // oldest first
            });

            int remaining_qty = quantity;
            double total_fill_value = 0.0;
            int total_fill_qty = 0;

            auto it = asks.begin();
            while (it != asks.end() && remaining_qty > 0) {
                if (order_type == "limit" && it->price > price) {
                    break; // ask price exceeds buy limit
                }
                
                int match_qty = std::min(remaining_qty, it->quantity);
                remaining_qty -= match_qty;
                it->quantity -= match_qty;
                total_fill_qty += match_qty;
                total_fill_value += match_qty * it->price;

                res.matched_order_ids.push_back(it->order_id);

                if (it->quantity == 0) {
                    it = asks.erase(it);
                } else {
                    ++it;
                }
            }

            if (total_fill_qty > 0) {
                res.actual_fill_qty = total_fill_qty;
                res.actual_fill_price = total_fill_value / total_fill_qty;
                res.status = (remaining_qty == 0) ? "filled" : "partial_fill";
            }

            if (order_type == "limit" && remaining_qty > 0) {
                new_order.quantity = remaining_qty;
                bids.push_back(new_order);
                if (total_fill_qty == 0) {
                    res.status = "ack";
                }
            } else if (order_type == "market" && remaining_qty > 0 && total_fill_qty == 0) {
                res.status = "rejected";
            }

        } else if (side == "sell") {
            // Match against bids (buyers)
            std::sort(bids.begin(), bids.end(), [](const BookOrder& a, const BookOrder& b) {
                if (a.price != b.price) return a.price > b.price; // highest price first
                return a.timestamp < b.timestamp;                 // oldest first
            });

            int remaining_qty = quantity;
            double total_fill_value = 0.0;
            int total_fill_qty = 0;

            auto it = bids.begin();
            while (it != bids.end() && remaining_qty > 0) {
                if (order_type == "limit" && it->price < price) {
                    break; // bid price below sell limit
                }

                int match_qty = std::min(remaining_qty, it->quantity);
                remaining_qty -= match_qty;
                it->quantity -= match_qty;
                total_fill_qty += match_qty;
                total_fill_value += match_qty * it->price;

                res.matched_order_ids.push_back(it->order_id);

                if (it->quantity == 0) {
                    it = bids.erase(it);
                } else {
                    ++it;
                }
            }

            if (total_fill_qty > 0) {
                res.actual_fill_qty = total_fill_qty;
                res.actual_fill_price = total_fill_value / total_fill_qty;
                res.status = (remaining_qty == 0) ? "filled" : "partial_fill";
            }

            if (order_type == "limit" && remaining_qty > 0) {
                new_order.quantity = remaining_qty;
                asks.push_back(new_order);
                if (total_fill_qty == 0) {
                    res.status = "ack";
                }
            } else if (order_type == "market" && remaining_qty > 0 && total_fill_qty == 0) {
                res.status = "rejected";
            }
        }

        return res;
    }
};

OrderBook global_book;

void handle_client(int client_fd) {
    char buffer[4096];
    std::memset(buffer, 0, sizeof(buffer));
    
    int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0) {
        close(client_fd);
        return;
    }
    
    std::string req(buffer);
    size_t body_pos = req.find("\r\n\r\n");
    if (body_pos == std::string::npos) {
        body_pos = req.find("\n\n");
        if (body_pos != std::string::npos) {
            body_pos += 2;
        }
    } else {
        body_pos += 4;
    }
    
    std::string body = "";
    if (body_pos != std::string::npos && body_pos < req.size()) {
        body = req.substr(body_pos);
    }
    
    std::string order_id = get_json_string(body, "order_id");
    std::string order_type = get_json_string(body, "order_type");
    std::string side = get_json_string(body, "side");
    double price = get_json_numeric(body, "price");
    double quantity = get_json_numeric(body, "quantity");
    
    MatchResult result = global_book.process_order(order_id, order_type, side, price, (int)quantity);
    
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    long long nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    
    std::ostringstream json_resp;
    json_resp << "{"
              << "\"order_id\":\"" << order_id << "\","
              << "\"status\":\"" << result.status << "\","
              << "\"acked_at_ns\":" << nanoseconds << ","
              << "\"expected_fill_qty\":" << (int)quantity << ","
              << "\"actual_fill_qty\":" << result.actual_fill_qty << ","
              << "\"expected_fill_price\":" << price << ","
              << "\"actual_fill_price\":" << result.actual_fill_price << ","
              << "\"reject_reason\":\"\",";
              
    json_resp << "\"matched_order_ids\":[";
    for (size_t i = 0; i < result.matched_order_ids.size(); ++i) {
        json_resp << "\"" << result.matched_order_ids[i] << "\"";
        if (i < result.matched_order_ids.size() - 1) json_resp << ",";
    }
    json_resp << "]}";
              
    std::string response_body = json_resp.str();
    
    std::ostringstream http_resp;
    http_resp << "HTTP/1.1 200 OK\r\n"
              << "Content-Type: application/json\r\n"
              << "Content-Length: " << response_body.size() << "\r\n"
              << "Connection: keep-alive\r\n"
              << "\r\n"
              << response_body;
              
    std::string response_str = http_resp.str();
    write(client_fd, response_str.c_str(), response_str.size());
    close(client_fd);
}

// Fixed ThreadPool to sustain high TPS without resource starvation
class ThreadPool {
public:
    ThreadPool(size_t threads) {
        for(size_t i = 0; i < threads; ++i)
            workers.emplace_back([this] {
                for(;;) {
                    int client_fd;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        if(this->stop && this->tasks.empty())
                            return;
                        client_fd = this->tasks.front();
                        this->tasks.pop();
                    }
                    handle_client(client_fd);
                }
            });
    }
    void enqueue(int client_fd) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.push(client_fd);
        }
        condition.notify_one();
    }
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for(std::thread &worker: workers) {
            if(worker.joinable()) worker.join();
        }
    }
private:
    std::vector<std::thread> workers;
    std::queue<int> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop = false;
};

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        return 1;
    }
    
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);
    
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        return 1;
    }
    
    if (listen(server_fd, 4096) < 0) {
        return 1;
    }
    
    std::cout << "Optimized C++ Order Matching Engine listening on :8080" << std::endl;
    
    // Allocate 8 concurrent worker threads
    ThreadPool pool(8);
    
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd >= 0) {
            pool.enqueue(client_fd);
        }
    }
    
    close(server_fd);
    return 0;
}
