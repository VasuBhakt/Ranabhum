resource "google_container_cluster" "gke_cluster" {
  name     = "ranabhum-gke"
  location = var.gcp_region

  network    = google_compute_network.vpc_network.id
  subnetwork = google_compute_subnetwork.subnet.id

  # We delete the default node pool to configure a custom, cost-optimized node pool
  remove_default_node_pool = true
  initial_node_count       = 1

  ip_allocation_policy {}

  deletion_protection = false
}

resource "google_container_node_pool" "primary_nodes" {
  name       = "ranabhum-node-pool"
  location   = var.gcp_region
  cluster    = google_container_cluster.gke_cluster.name
  node_count = 2

  node_config {
    preemptible  = true       # Reduces VM cost by up to 70% (perfect for hackathons)
    machine_type = "e2-medium" # Sized correctly to run the stack + DinD sandboxes

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}
