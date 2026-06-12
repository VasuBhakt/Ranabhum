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
├── tests/                    # Local integration tests and mock contestant files
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
Make sure Docker is running on your host machine. In the root directory:
```bash
docker compose up -d --build
```
*(All service environment variables are loaded dynamically from the root `.env` file.)*

### 2. Verify local developer portals:
* 🏆 **React Leaderboard Dashboard**: [http://localhost:8082](http://localhost:8082)
* 📊 **Redpanda Kafka Console**: [http://localhost:8081](http://localhost:8081)
* 🔌 **Sandbox Engine API**: [http://localhost:8080](http://localhost:8080)
* 💾 **Telemetry REST API**: [http://localhost:8001/health](http://localhost:8001/health)

### 3. Run a test submission:
Send a mock contestant zip submission (which contains an optimized order matching server in Go, C++, or Rust) to the Sandbox Engine:

```bash
# Go submission (titans)
curl -F "team_name=titans" -F "language=go" -F "file=@./tests/test_submission.zip" http://localhost:8080/submit

# C++ submission (gladiators)
curl -F "team_name=gladiators" -F "language=cpp" -F "file=@./tests/cpp_submission.zip" http://localhost:8080/submit

# Rust submission (vikings)
curl -F "team_name=vikings" -F "language=rust" -F "file=@./tests/rust_submission.zip" http://localhost:8080/submit
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
