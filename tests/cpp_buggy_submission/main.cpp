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
static std::vector<BookOrder> bids;
static std::vector<BookOrder> asks;

static int request_count = 0;

static MatchResult process_order(const std::string& id, const std::string& type,
                                  const std::string& side, double price, int qty) {
    std::lock_guard<std::mutex> lock(book_mutex);

    request_count++;
    if (request_count % 10 == 0) {
        // Deliberate BUG: Order rejected, but maliciously reports actual_fill_qty == qty
        // to test if the scoring engine correctly flags this as incorrect!
        return MatchResult{"rejected", qty, price, {}};
    }

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
        std::sort(asks.begin(), asks.end(), [](const BookOrder& a, const BookOrder& b) {
            return a.price != b.price ? a.price < b.price : a.timestamp < b.timestamp;
        });

        auto it = asks.begin();
        while (it != asks.end() && remaining > 0) {
            if (type == "limit" && it->price > price) break;
            int fill = std::min(remaining, it->quantity);
            remaining -= fill;
            total_fill += fill;
            total_value += fill * it->price;
            res.matched_ids.push_back(it->id);
            it->quantity -= fill;
            if (it->quantity == 0) it = asks.erase(it);
            else ++it;
        }

        if (total_fill > 0) {
            res.actual_fill_qty = total_fill;
            res.actual_fill_price = total_value / total_fill;
            res.status = (remaining == 0) ? "filled" : "partial_fill";
        }

        if (type == "limit" && remaining > 0) {
            bids.push_back({id, side, price, remaining, now});
            if (total_fill == 0) res.status = "ack";
        } else if (type == "market" && remaining > 0 && total_fill == 0) {
            res.status = "rejected";
        }
    } else {
        std::sort(bids.begin(), bids.end(), [](const BookOrder& a, const BookOrder& b) {
            return a.price != b.price ? a.price > b.price : a.timestamp < b.timestamp;
        });

        auto it = bids.begin();
        while (it != bids.end() && remaining > 0) {
            if (type == "limit" && it->price < price) break;
            int fill = std::min(remaining, it->quantity);
            remaining -= fill;
            total_fill += fill;
            total_value += fill * it->price;
            res.matched_ids.push_back(it->id);
            it->quantity -= fill;
            if (it->quantity == 0) it = bids.erase(it);
            else ++it;
        }

        if (total_fill > 0) {
            res.actual_fill_qty = total_fill;
            res.actual_fill_price = total_value / total_fill;
            res.status = (remaining == 0) ? "filled" : "partial_fill";
        }

        if (type == "limit" && remaining > 0) {
            asks.push_back({id, side, price, remaining, now});
            if (total_fill == 0) res.status = "ack";
        } else if (type == "market" && remaining > 0 && total_fill == 0) {
            res.status = "rejected";
        }
    }

    if (res.status.empty()) {
        res.status = (total_fill > 0) ? "partial_fill" : "ack";
    }

    return res;
}

// ──────────────────── HTTP handling ────────────────────

// Read exactly one full HTTP request from the buffer, returning body and
// advancing the buffer past the consumed request. Returns false if more
// data is needed or the connection should be closed.
static bool read_request(int fd, std::string& buf, std::string& body_out) {
    char tmp[4096];
    while (true) {
        // Check if we have headers
        auto hdr_end = buf.find("\r\n\r\n");
        if (hdr_end != std::string::npos) {
            // Find Content-Length
            size_t cl = 0;
            auto cl_pos = buf.find("Content-Length: ");
            if (cl_pos != std::string::npos && cl_pos < hdr_end) {
                cl = std::strtoul(buf.c_str() + cl_pos + 16, nullptr, 10);
            }
            size_t total = hdr_end + 4 + cl;
            if (buf.size() >= total) {
                body_out = buf.substr(hdr_end + 4, cl);
                buf.erase(0, total);
                return true;
            }
        }
        // Need more data
        ssize_t n = read(fd, tmp, sizeof(tmp));
        if (n <= 0) return false; // EOF or error
        buf.append(tmp, n);
    }
}

static void send_response(int fd, const MatchResult& res,
                           const std::string& order_id, int qty, double price) {
    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Build matched_order_ids JSON array
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

    write(fd, header, hdr_len);
    write(fd, body, body_len);
}

static void handle_client(int fd) {
    std::string buf;
    std::string body;

    // Keep-alive loop: process multiple requests on the same connection
    while (read_request(fd, buf, body)) {
        auto order_id  = json_str(body, "order_id");
        auto order_type = json_str(body, "order_type");
        auto side      = json_str(body, "side");
        double price   = json_num(body, "price");
        int quantity   = json_int(body, "quantity");

        auto result = process_order(order_id, order_type, side, price, quantity);
        send_response(fd, result, order_id, quantity, price);
    }

    close(fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 128);

    printf("C++ Order Matching Engine listening on :8080\n");
    fflush(stdout);

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;
        std::thread(handle_client, client_fd).detach();
    }
}
