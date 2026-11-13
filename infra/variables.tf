variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix for all resources."
  type        = string
  default     = "jobplatform"
}

variable "environment" {
  description = "Environment label, for example dev or prod."
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones to spread subnets over (2 or 3)."
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be 2 or 3."
  }
}

variable "image_tag" {
  description = "Tag of the application image in ECR."
  type        = string
  default     = "latest"
}

variable "certificate_arn" {
  description = "Optional ACM certificate ARN. When set, the ALB serves HTTPS and redirects HTTP."
  type        = string
  default     = ""
}

variable "alarm_email" {
  description = "Optional email address subscribed to the alarm topic."
  type        = string
  default     = ""
}

# --- application sizing -------------------------------------------------------------

variable "api_desired_count" {
  type    = number
  default = 2
}

variable "worker_desired_count" {
  type    = number
  default = 2
}

variable "worker_max_count" {
  description = "Upper bound for worker autoscaling."
  type        = number
  default     = 10
}

variable "relay_desired_count" {
  description = "Relays are safe to run concurrently (SKIP LOCKED), two gives availability."
  type        = number
  default     = 2
}

variable "reaper_desired_count" {
  type    = number
  default = 1
}

variable "api_cpu" {
  type    = number
  default = 512
}

variable "api_memory" {
  type    = number
  default = 1024
}

variable "worker_cpu" {
  type    = number
  default = 1024
}

variable "worker_memory" {
  type    = number
  default = 2048
}

variable "small_cpu" {
  description = "CPU units for relay and reaper."
  type        = number
  default     = 256
}

variable "small_memory" {
  description = "Memory (MiB) for relay and reaper."
  type        = number
  default     = 512
}

variable "worker_concurrency" {
  type    = number
  default = 32
}

variable "worker_cpu_processes" {
  description = "Process pool size for CPU-bound jobs. Keep at or below the vCPU count of the task."
  type        = number
  default     = 1
}

variable "log_retention_days" {
  type    = number
  default = 14
}

# --- data stores ---------------------------------------------------------------------

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_allocated_storage" {
  type    = number
  default = 20
}

variable "db_multi_az" {
  type    = bool
  default = false
}

variable "db_deletion_protection" {
  description = "Set true for anything you care about. Must be false to run terraform destroy."
  type        = bool
  default     = false
}

variable "db_skip_final_snapshot" {
  type    = bool
  default = true
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.micro"
}

variable "mq_instance_type" {
  type    = string
  default = "mq.t3.micro"
}
