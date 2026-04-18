variable "account_id" {
  description = "Cloudflare account ID"
  type        = string
}

variable "zone_id" {
  description = "Cloudflare zone ID. run.sh can resolve this automatically when omitted."
  type        = string
  default     = null
}

variable "domain" {
  description = "Primary domain hosted in Cloudflare"
  type        = string
}

variable "pages_project_name" {
  description = "Cloudflare Pages project name"
  type        = string
  default     = "tabular-ui"
}

variable "pages_branch" {
  description = "Repository branch used for production deployments"
  type        = string
  default     = "main"
}

variable "pages_repo_owner" {
  description = "GitHub owner or organization for the Pages repository"
  type        = string
}

variable "pages_repo_name" {
  description = "GitHub repository name for the Pages project"
  type        = string
}

variable "pages_repo_id" {
  description = "GitHub repository ID. run.sh can resolve this automatically when omitted."
  type        = string
  default     = null
}

variable "pages_root_dir" {
  description = "Cloudflare Pages root directory"
  type        = string
  default     = "."
}

variable "pages_destination_dir" {
  description = "Cloudflare Pages output directory"
  type        = string
  default     = "dist"
}

variable "tunnel_name" {
  description = "Cloudflare Tunnel name"
  type        = string
  default     = "tabular-api-tunnel"
}

variable "rate_limit_enabled" {
  description = "Enable or disable the zone rate limiting ruleset"
  type        = bool
  default     = true
}

variable "rate_limit_action" {
  description = "Rate limiting action. For non-Enterprise plans, managed_challenge with mitigation_timeout=0 is safest."
  type        = string
  default     = "managed_challenge"

  validation {
    condition     = contains(["block", "js_challenge", "managed_challenge", "challenge", "log"], var.rate_limit_action)
    error_message = "rate_limit_action must be one of: block, js_challenge, managed_challenge, challenge, log."
  }
}

variable "rate_limit_requests" {
  description = "Maximum requests allowed in the configured period"
  type        = number
  default     = 60
}

variable "rate_limit_period" {
  description = "Rate limiting window in seconds"
  type        = number
  default     = 60
}

variable "rate_limit_mitigation_timeout" {
  description = "Mitigation timeout in seconds"
  type        = number
  default     = 0
}