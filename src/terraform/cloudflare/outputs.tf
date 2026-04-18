output "frontend_url" {
  value = "https://${local.app_hostname}"
}

output "pages_project_name" {
  value = cloudflare_pages_project.frontend.name
}