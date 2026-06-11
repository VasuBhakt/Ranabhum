output "cluster_name" {
  description = "GKE Cluster Name"
  value       = google_container_cluster.gke_cluster.name
}

output "cluster_endpoint" {
  description = "GKE Cluster Endpoint"
  value       = google_container_cluster.gke_cluster.endpoint
}
