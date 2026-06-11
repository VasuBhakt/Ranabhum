resource "google_compute_network" "vpc_network" {
  name                    = "ranabhum-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "ranabhum-subnet"
  ip_cidr_range = "10.0.1.0/24"
  region        = var.gcp_region
  network       = google_compute_network.vpc_network.id

  # Enables private access to Google APIs for pods (required for GKE Autopilot)
  private_ip_google_access = true
}
