locals {
  app_hostname = "app.${var.domain}"
}

resource "cloudflare_pages_project" "frontend" {
  account_id       = var.account_id
  name             = var.pages_project_name
  production_branch = var.pages_branch

  build_config = {
    build_caching   = true
    build_command   = "rm -rf ${var.pages_destination_dir} && mkdir -p ${var.pages_destination_dir} && cp -R src/frontend/. ${var.pages_destination_dir}/"
    destination_dir = var.pages_destination_dir
    root_dir        = var.pages_root_dir
  }

  source = {
    type = "github"

    config = {
      owner                          = var.pages_repo_owner
      repo_id                        = var.pages_repo_id
      repo_name                      = var.pages_repo_name
      production_branch              = var.pages_branch
      production_deployments_enabled = true
      pr_comments_enabled            = true
    }
  }
}

resource "cloudflare_pages_domain" "frontend_domain" {
  account_id   = var.account_id
  project_name = cloudflare_pages_project.frontend.name
  name         = local.app_hostname

  depends_on = [cloudflare_pages_project.frontend]
}

resource "cloudflare_dns_record" "frontend_cname" {
  zone_id = var.zone_id
  name    = local.app_hostname
  type    = "CNAME"
  content = cloudflare_pages_project.frontend.subdomain
  proxied = true
  ttl     = 1

  depends_on = [cloudflare_pages_domain.frontend_domain]
}