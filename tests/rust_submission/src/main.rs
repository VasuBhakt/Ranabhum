use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

fn get_json_string(json: &str, key: &str) -> String {
    let search_key = format!("\"{}\"", key);
    if let Some(pos) = json.find(&search_key) {
        if let Some(colon_pos) = json[pos..].find(':') {
            let actual_colon = pos + colon_pos;
            if let Some(start_quote) = json[actual_colon..].find('"') {
                let actual_start = actual_colon + start_quote + 1;
                if let Some(end_quote) = json[actual_start..].find('"') {
                    return json[actual_start..(actual_start + end_quote)].to_string();
                }
            }
        }
    }
    String::new()
}

fn get_json_numeric(json: &str, key: &str) -> f64 {
    let search_key = format!("\"{}\"", key);
    if let Some(pos) = json.find(&search_key) {
        if let Some(colon_pos) = json[pos..].find(':') {
            let actual_colon = pos + colon_pos;
            let val_str: String = json[actual_colon + 1..]
                .chars()
                .take_while(|c| c.is_digit(10) || *c == '.' || *c == '-' || c.is_whitespace())
                .filter(|c| !c.is_whitespace())
                .collect();
            return val_str.parse().unwrap_or(0.0);
        }
    }
    0.0
}

#[derive(Clone, Debug)]
struct BookOrder {
    order_id: String,
    side: String,
    price: f64,
    quantity: i32,
    timestamp: u64,
}

#[derive(Debug)]
struct MatchResult {
    actual_fill_qty: i32,
    actual_fill_price: f64,
    status: String,
}

// Stateful order book simulating actual matching priority
struct OrderBook {
    bids: Vec<BookOrder>, // buy orders
    asks: Vec<BookOrder>, // sell orders
}

impl OrderBook {
    fn new() -> Self {
        OrderBook {
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn process_order(&mut self, order_id: &str, order_type: &str, side: &str, price: f64, quantity: i32) -> MatchResult {
        if order_type == "cancel" {
            let mut found = false;
            if let Some(idx) = self.bids.iter().position(|o| o.order_id == order_id) {
                self.bids.remove(idx);
                found = true;
            }
            if !found {
                if let Some(idx) = self.asks.iter().position(|o| o.order_id == order_id) {
                    self.asks.remove(idx);
                    found = true;
                }
            }
            return MatchResult {
                actual_fill_qty: 0,
                actual_fill_price: 0.0,
                status: if found { "cancelled".to_string() } else { "rejected".to_string() },
            };
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let new_order = BookOrder {
            order_id: order_id.to_string(),
            side: side.to_string(),
            price,
            quantity,
            timestamp: now,
        };

        let mut res = MatchResult {
            actual_fill_qty: 0,
            actual_fill_price: 0.0,
            status: "ack".to_string(),
        };

        if side == "buy" {
            // Sort asks: lowest price first, then oldest first
            self.asks.sort_by(|a, b| {
                if a.price != b.price {
                    a.price.partial_cmp(&b.price).unwrap()
                } else {
                    a.timestamp.cmp(&b.timestamp)
                }
            });

            let mut remaining_qty = quantity;
            let mut total_fill_value = 0.0;
            let mut total_fill_qty = 0;

            let mut idx = 0;
            while idx < self.asks.len() && remaining_qty > 0 {
                if order_type == "limit" && self.asks[idx].price > price {
                    break; // seller wants more than our buy limit
                }

                let match_qty = remaining_qty.min(self.asks[idx].quantity);
                remaining_qty -= match_qty;
                self.asks[idx].quantity -= match_qty;
                total_fill_qty += match_qty;
                total_fill_value += match_qty as f64 * self.asks[idx].price;

                if self.asks[idx].quantity == 0 {
                    self.asks.remove(idx);
                } else {
                    idx += 1;
                }
            }

            if total_fill_qty > 0 {
                res.actual_fill_qty = total_fill_qty;
                res.actual_fill_price = total_fill_value / total_fill_qty as f64;
                res.status = if remaining_qty == 0 { "filled".to_string() } else { "partial_fill".to_string() };
            }

            if order_type == "limit" && remaining_qty > 0 {
                let mut remainder = new_order;
                remainder.quantity = remaining_qty;
                self.bids.push(remainder);
                if total_fill_qty == 0 {
                    res.status = "ack".to_string();
                }
            } else if order_type == "market" && remaining_qty > 0 && total_fill_qty == 0 {
                res.status = "rejected".to_string();
            }

        } else if side == "sell" {
            // Sort bids: highest price first, then oldest first
            self.bids.sort_by(|a, b| {
                if a.price != b.price {
                    b.price.partial_cmp(&a.price).unwrap() // highest price first
                } else {
                    a.timestamp.cmp(&b.timestamp)
                }
            });

            let mut remaining_qty = quantity;
            let mut total_fill_value = 0.0;
            let mut total_fill_qty = 0;

            let mut idx = 0;
            while idx < self.bids.len() && remaining_qty > 0 {
                if order_type == "limit" && self.bids[idx].price < price {
                    break; // buyer wants to pay less than our sell limit
                }

                let match_qty = remaining_qty.min(self.bids[idx].quantity);
                remaining_qty -= match_qty;
                self.bids[idx].quantity -= match_qty;
                total_fill_qty += match_qty;
                total_fill_value += match_qty as f64 * self.bids[idx].price;

                if self.bids[idx].quantity == 0 {
                    self.bids.remove(idx);
                } else {
                    idx += 1;
                }
            }

            if total_fill_qty > 0 {
                res.actual_fill_qty = total_fill_qty;
                res.actual_fill_price = total_fill_value / total_fill_qty as f64;
                res.status = if remaining_qty == 0 { "filled".to_string() } else { "partial_fill".to_string() };
            }

            if order_type == "limit" && remaining_qty > 0 {
                let mut remainder = new_order;
                remainder.quantity = remaining_qty;
                self.asks.push(remainder);
                if total_fill_qty == 0 {
                    res.status = "ack".to_string();
                }
            } else if order_type == "market" && remaining_qty > 0 && total_fill_qty == 0 {
                res.status = "rejected".to_string();
            }
        }

        res
    }
}

// Global OrderBook wrap in Arc<Mutex>
lazy_static::lazy_static! {
    static ref ORDER_BOOK: Arc<Mutex<OrderBook>> = Arc::new(Mutex::new(OrderBook::new()));
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 4096];
    if let Ok(bytes_read) = stream.read(&mut buffer) {
        if bytes_read == 0 {
            return;
        }
        
        let req = String::from_utf8_lossy(&buffer[..bytes_read]);
        let body = if let Some(pos) = req.find("\r\n\r\n") {
            &req[pos + 4..]
        } else if let Some(pos) = req.find("\n\n") {
            &req[pos + 2..]
        } else {
            ""
        };
        
        let order_id = get_json_string(body, "order_id");
        let order_type = get_json_string(body, "order_type");
        let side = get_json_string(body, "side");
        let price = get_json_numeric(body, "price");
        let quantity = get_json_numeric(body, "quantity") as i32;
        
        let match_result = {
            let mut book = ORDER_BOOK.lock().unwrap();
            book.process_order(&order_id, &order_type, &side, price, quantity)
        };
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
            
        let response_body = format!(
            "{{\"order_id\":\"{}\",\"status\":\"{}\",\"acked_at_ns\":{},\"expected_fill_qty\":{},\"actual_fill_qty\":{},\"expected_fill_price\":{},\"actual_fill_price\":{},\"reject_reason\":\"\"}}",
            order_id, match_result.status, now, quantity, match_result.actual_fill_qty, price, match_result.actual_fill_price
        );
        
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        
        let _ = stream.write_all(response.as_bytes());
    }
}

// Clean Rust ThreadPool to prevent thread limits under load
struct ThreadPool {
    sender: mpsc::Sender<TcpStream>,
}

impl ThreadPool {
    fn new(size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<TcpStream>();
        let receiver = Arc::new(Mutex::new(receiver));
        
        for _ in 0..size {
            let rx = Arc::clone(&receiver);
            thread::spawn(move || loop {
                let stream = rx.lock().unwrap().recv();
                match stream {
                    Ok(stream) => {
                        handle_client(stream);
                    }
                    Err(_) => break,
                }
            });
        }
        
        ThreadPool { sender }
    }

    fn execute(&self, stream: TcpStream) {
        let _ = self.sender.send(stream);
    }
}

fn main() {
    let listener = TcpListener::bind("0.0.0.0:8080").expect("Failed to bind to port 8080");
    println!("Optimized Rust Order Matching Engine listening on :8080");
    
    let pool = ThreadPool::new(8); // 8 worker threads
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                pool.execute(stream);
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}
