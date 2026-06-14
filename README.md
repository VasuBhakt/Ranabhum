# Ranabhum 🏹
> **High-Performance Benchmarking & Telemetry Platform for Order Matching Engines**
> Built for the IICPC Hackathon 2026

Ranabhum is a highly scalable, event-driven benchmark and real-time telemetry pipeline designed to compile, isolate, stress-test, and score high-performance financial order matching engines submitted by contestants.

---

## 🛠️ Tech Stack & Badges
* **Language/Runtimes**: Go (Bot Fleet, Sandbox Engine), Python / FastAPI (Telemetry API & Consumer), React / TypeScript (Frontend)
* **Databases & Messaging**: TimescaleDB (Postgres), Redis (Leaderboard Cache), Redpanda (Kafka Compatibility)
* **Infrastructure**: Docker, Kubernetes, Terraform

---

## 📂 Project Structure
```text
├── botfleet/                 # Load generator bots and coordinator (Go)
│   ├── cmd/                  # Executable entrypoints (coordinator, worker)
│   └── internal/             # Core benchmark logic, Kafka publishers, & models
├── tests/                    # Mock contestant submissions (Go) for platform validation
├── sandbox-engine/           # Submission compiler and container runner (Go)
├── telemetry/                # Analytics and metric aggregation service (Python)
│   ├── app/                  # FastAPI endpoints, TimescaleDB schema, consumer loops
│   └── frontend/             # Leaderboard UI (React + TypeScript + WebSockets)
├── k8s/                      # Cloud-agnostic Kubernetes manifests (yaml)
├── terraform/                # Terraform GCP provisioning scripts (HCL)
├── docker-compose.yml        # Unified local stack deployment configuration
├── ARCHITECTURE.md           # In-depth architectural topography and scale optimizations
└── README.md                 # This documentation file
```

---

## 🚀 Quickstart: Local Setup (Docker Compose)

The entire platform can be brought up locally in a single command.

### 1. Spin up the entire infrastructure:
Make sure Docker is running on your host machine. In the root directory, first create your environment file:
```bash
cp .env.example .env
```
Then start the stack:
```bash
docker compose up -d --build
```
*(All service environment variables are loaded dynamically from the root `.env` file you just created.)*

### 2. Verify local developer portals:
* 🏆 **React Leaderboard Dashboard**: [http://localhost:8082](http://localhost:8082)
* 📊 **Redpanda Kafka Console**: [http://localhost:8081](http://localhost:8081)
* 🔌 **Sandbox Engine API**: [http://localhost:8080](http://localhost:8080)
* 💾 **Telemetry REST API**: [http://localhost:8001/health](http://localhost:8001/health)

### 3. Run test submissions:
Four mock submissions are provided (in both Go and C++) with different performance profiles to validate leaderboard differentiation and scoring engine correctness:

```bash
# 1. Go Fast Engine — Basic matching, decent TPS
curl -F "team_name=vikings" -F "language=go" -F "file=@./tests/go_slow_submission.zip" http://localhost:8080/submit

# 2. Go Orderbook Engine — Real price-time priority matching, higher TPS
curl -F "team_name=phoenix" -F "language=go" -F "file=@./tests/go_orderbook_submission.zip" http://localhost:8080/submit

# 3. C++ Orderbook Engine — Real matching, NGINX-style multi-threaded epoll architecture, absolute highest TPS
curl -F "team_name=gladiators" -F "language=cpp" -F "file=@./tests/cpp_submission.zip" http://localhost:8080/submit

# 4. C++ Buggy Engine — Thread-per-connection architecture. Triggers high memory usage and contains a deliberate bug (reports false fills on rejected orders). Tests scoring penalty and OOM timeouts!
curl -F "team_name=romans" -F "language=cpp" -F "file=@./tests/cpp_buggy_submission.zip" http://localhost:8080/submit
```

### 4. Monitor live telemetry consumption:
Watch the consumer ingest telemetry in batches and calculate final scores:
```bash
docker compose logs -f telemetry-consumer
```

---

## ☁️ Cloud Deployment Setup (Google Cloud GKE)

Ranabhum is prepared to deploy to Google Cloud Platform (GCP). For GKE Standard clusters, VM node pools are configured via Terraform.

### 1. Provision GCP Infrastructure with Terraform:
```bash
cd terraform
terraform init
terraform apply -var="gcp_project_id=YOUR_GCP_PROJECT_ID"
```

### 2. Authenticate `kubectl` to GKE:
```bash
gcloud container clusters get-credentials ranabhum-gke --region us-central1 --project YOUR_GCP_PROJECT_ID
```

### 3. Apply Kubernetes Manifests:
```bash
cd ../k8s
kubectl apply -f .
```
*(Contestant containers are run securely inside GKE using a custom **Docker-in-Docker (DinD)** sidecar to isolate worker VM node execution).*

---

## 📘 Architecture & Scaling Blueprint
For details on system topography, technology rationales, async-safe buffering, socket throttling, database pooling, and GKE security designs, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 👥 Contributors
1. **Prathamesh Prasad**
2. **Swastik Bose**
3. **Kiran Patra**
