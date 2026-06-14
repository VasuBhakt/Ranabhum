// Simple C++ HTTP server for order matching engine benchmarking.
// Uses thread-per-connection with keep-alive. Works with both persistent
// connections (pooled proxy) and one-shot connections (legacy proxy).
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <mutex>
#include <vector>
#include <algorithm>
#include <chrono>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unordered_map>
#include <unistd.h>
// ──────────────────── JSON helpers ────────────────────
static std::string json_str(const std::string& json, const char* key) {
    std::string needle = std::string("\"") + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return "";
    auto colon = json.find(':', pos + needle.size());
    if (colon == std::string::npos) return "";
    auto q1 = json.find('"', colon + 1);
    if (q1 == std::string::npos) return "";
    auto q2 = json.find('"', q1 + 1);
    if (q2 == std::string::npos) return "";
    return json.substr(q1 + 1, q2 - q1 - 1);
}

static double json_num(const std::string& json, const char* key) {
    std::string needle = std::string("\"") + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return 0;
    auto colon = json.find(':', pos + needle.size());
    if (colon == std::string::npos) return 0;
    return std::strtod(json.c_str() + colon + 1, nullptr);
}

static int json_int(const std::string& json, const char* key) {
    return static_cast<int>(json_num(json, key));
}

// ──────────────────── Order Book ────────────────────
struct BookOrder {
    std::string id;
    std::string side;
    double price;
    int quantity;
    int64_t timestamp;
};

struct MatchResult {
    std::string status;
    int actual_fill_qty;
    double actual_fill_price;
    std::vector<std::string> matched_ids;
};

static std::mutex book_mutex;
#include <map>
#include <deque>

// We need a custom comparator to sort bids highest-first
static std::map<double, std::deque<BookOrder>, std::greater<double>> bids;
// Asks are sorted lowest-first (default less)
static std::map<double, std::deque<BookOrder>> asks;

static MatchResult process_order(const std::string& id, const std::string& type,
                                  const std::string& side, double price, int qty) {
    std::lock_guard<std::mutex> lock(book_mutex);

    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    MatchResult res{"ack", 0, 0.0, {}};

    if (type == "cancel") {
        res.status = "ack";
        return res;
    }

    int remaining = qty;
    int total_fill = 0;
    double total_value = 0.0;

    if (side == "buy") {
        auto it = asks.begin();
        while (it != asks.end() && remaining > 0) {
            if (type == "limit" && it->first > price) break;
            
            auto& queue = it->second;
            while (!queue.empty() && remaining > 0) {
                auto& top_order = queue.front();
                int fill = std::min(remaining, top_order.quantity);
                remaining -= fill;
                total_fill += fill;
                total_value += fill * top_order.price;
                res.matched_ids.push_back(top_order.id);
                top_order.quantity -= fill;

                if (top_order.quantity == 0) {
                    queue.pop_front();
                }
            }
            
            if (queue.empty()) {
                it = asks.erase(it);
            } else {
                ++it;
            }
        }

        if (total_fill > 0) {
            res.status = remaining == 0 ? "filled" : "partial_fill";
            res.actual_fill_qty = total_fill;
            res.actual_fill_price = total_value / total_fill;
        }

        if (remaining > 0 && type == "limit") {
            bids[price].push_back({id, side, price, remaining, now});
            if (total_fill == 0) res.status = "ack";
        } else if (remaining > 0 && type == "market" && total_fill == 0) {
            res.status = "rejected";
        }
    } else if (side == "sell") {
        auto it = bids.begin();
        while (it != bids.end() && remaining > 0) {
            if (type == "limit" && it->first < price) break;
            
            auto& queue = it->second;
            while (!queue.empty() && remaining > 0) {
                auto& top_order = queue.front();
                int fill = std::min(remaining, top_order.quantity);
                remaining -= fill;
                total_fill += fill;
                total_value += fill * top_order.price;
                res.matched_ids.push_back(top_order.id);
                top_order.quantity -= fill;

                if (top_order.quantity == 0) {
                    queue.pop_front();
                }
            }

            if (queue.empty()) {
                it = bids.erase(it);
            } else {
                ++it;
            }
        }

        if (total_fill > 0) {
            res.status = remaining == 0 ? "filled" : "partial_fill";
            res.actual_fill_qty = total_fill;
            res.actual_fill_price = total_value / total_fill;
        }

        if (remaining > 0 && type == "limit") {
            asks[price].push_back({id, side, price, remaining, now});
            if (total_fill == 0) res.status = "ack";
        } else if (remaining > 0 && type == "market" && total_fill == 0) {
            res.status = "rejected";
        }
    }

    if (res.status.empty()) {
        res.status = (total_fill > 0) ? "partial_fill" : "ack";
    }

    return res;
}

// ──────────────────── HTTP handling ────────────────────

#include <poll.h>

// ──────────────────── epoll HTTP Server ────────────────────

struct ClientState {
    int fd;
    std::string buf;
};

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void write_all(int fd, const char* buf, int len) {
    int total = 0;
    while (total < len) {
        int n = write(fd, buf + total, len - total);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd{};
                pfd.fd = fd;
                pfd.events = POLLOUT;
                poll(&pfd, 1, -1);
                continue;
            }
            break; // Socket error, client disconnected
        }
        total += n;
    }
}

static void send_response(int fd, const MatchResult& res,
                           const std::string& order_id, int qty, double price) {
    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    std::string matched = "[";
    for (size_t i = 0; i < res.matched_ids.size(); ++i) {
        if (i > 0) matched += ",";
        matched += "\"" + res.matched_ids[i] + "\"";
    }
    matched += "]";

    char body[2048];
    int body_len = snprintf(body, sizeof(body),
        "{\"order_id\":\"%s\",\"status\":\"%s\",\"acked_at_ns\":%ld,"
        "\"expected_fill_qty\":%d,\"actual_fill_qty\":%d,"
        "\"expected_fill_price\":%.2f,\"actual_fill_price\":%.2f,"
        "\"reject_reason\":\"\",\"matched_order_ids\":%s}",
        order_id.c_str(), res.status.c_str(), now,
        qty, res.actual_fill_qty, price, res.actual_fill_price,
        matched.c_str());

    char header[256];
    int hdr_len = snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %d\r\n"
        "Connection: keep-alive\r\n\r\n", body_len);

    write_all(fd, header, hdr_len);
    write_all(fd, body, body_len);
}

#include <thread>
#include <vector>

void run_worker(int server_fd) {
    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[4096];
    
    // EPOLLEXCLUSIVE prevents thundering herd when multiple threads listen to the same socket
    ev.events = EPOLLIN | EPOLLEXCLUSIVE;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::unordered_map<int, ClientState> clients;

    while (true) {
        int nfds = epoll_wait(epoll_fd, events, 4096, -1);
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            if (fd == server_fd) {
                while (true) {
                    int client_fd = accept(server_fd, nullptr, nullptr);
                    if (client_fd < 0) break;
                    set_nonblocking(client_fd);
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                    clients[client_fd] = {client_fd, ""};
                }
            } else {
                auto& client = clients[fd];
                char tmp[8192];
                bool error = false;
                
                while (true) {
                    ssize_t n = read(fd, tmp, sizeof(tmp));
                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        error = true;
                        break;
                    } else if (n == 0) {
                        error = true;
                        break;
                    } else {
                        client.buf.append(tmp, n);
                    }
                }

                if (!error) {
                    while (true) {
                        auto hdr_end = client.buf.find("\r\n\r\n");
                        if (hdr_end == std::string::npos) break;
                        
                        size_t cl = 0;
                        auto cl_pos = client.buf.find("Content-Length: ");
                        if (cl_pos != std::string::npos && cl_pos < hdr_end) {
                            cl = std::strtoul(client.buf.c_str() + cl_pos + 16, nullptr, 10);
                        }
                        size_t total = hdr_end + 4 + cl;
                        
                        if (client.buf.size() >= total) {
                            std::string body = client.buf.substr(hdr_end + 4, cl);
                            client.buf.erase(0, total);

                            auto order_id  = json_str(body, "order_id");
                            auto order_type = json_str(body, "order_type");
                            auto side      = json_str(body, "side");
                            double price   = json_num(body, "price");
                            int quantity   = json_int(body, "quantity");

                            auto result = process_order(order_id, order_type, side, price, quantity);
                            send_response(fd, result, order_id, quantity, price);
                        } else {
                            break;
                        }
                    }
                }

                if (error) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    clients.erase(fd);
                }
            }
        }
    }
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblocking(server_fd);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 8192);

    printf("C++ epoll Engine listening on :8080 (Multi-threaded NGINX Architecture)\n");
    fflush(stdout);

    // Spawn 2 workers to perfectly match the 2 CPU cores available to the Docker container
    std::thread t1(run_worker, server_fd);
    std::thread t2(run_worker, server_fd);

    t1.join();
    t2.join();
    return 0;
}
