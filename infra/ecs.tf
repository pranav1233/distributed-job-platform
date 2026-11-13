resource "aws_ecs_cluster" "main" {
  name = "${var.name}-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# --- IAM --------------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-${var.environment}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "read_secrets" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      aws_secretsmanager_secret.database_url.arn,
      aws_secretsmanager_secret.redis_url.arn,
      aws_secretsmanager_secret.rabbitmq_url.arn,
    ]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "read-app-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.read_secrets.json
}

# The application itself calls no AWS APIs, so the task role carries no permissions.
resource "aws_iam_role" "task" {
  name               = "${var.name}-${var.environment}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# --- task definitions -------------------------------------------------------------------

locals {
  image = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"

  services = {
    api = {
      command = ["python", "-m", "jobplatform.api"]
      cpu     = var.api_cpu
      memory  = var.api_memory
      desired = var.api_desired_count
      port    = 8000
    }
    worker = {
      command = ["python", "-m", "jobplatform.worker"]
      cpu     = var.worker_cpu
      memory  = var.worker_memory
      desired = var.worker_desired_count
      port    = 9101
    }
    relay = {
      command = ["python", "-m", "jobplatform.relay"]
      cpu     = var.small_cpu
      memory  = var.small_memory
      desired = var.relay_desired_count
      port    = 9102
    }
    reaper = {
      command = ["python", "-m", "jobplatform.reaper"]
      cpu     = var.small_cpu
      memory  = var.small_memory
      desired = var.reaper_desired_count
      port    = 9103
    }
  }

  common_environment = [
    { name = "LOG_LEVEL", value = "INFO" },
    { name = "LOG_JSON", value = "true" },
    { name = "WORKER_CONCURRENCY", value = tostring(var.worker_concurrency) },
    { name = "WORKER_PREFETCH", value = tostring(var.worker_concurrency * 2) },
    { name = "WORKER_CPU_PROCESSES", value = tostring(var.worker_cpu_processes) },
    { name = "LEASE_SECONDS", value = "30" },
    { name = "HEARTBEAT_INTERVAL_SECONDS", value = "10" },
    { name = "SHUTDOWN_GRACE_SECONDS", value = "50" },
  ]

  common_secrets = [
    { name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn },
    { name = "REDIS_URL", valueFrom = aws_secretsmanager_secret.redis_url.arn },
    { name = "RABBITMQ_URL", valueFrom = aws_secretsmanager_secret.rabbitmq_url.arn },
  ]
}

resource "aws_cloudwatch_log_group" "svc" {
  for_each          = merge(local.services, { migrate = {} })
  name              = "/ecs/${var.name}-${var.environment}/${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_task_definition" "svc" {
  for_each = local.services

  family                   = "${var.name}-${var.environment}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = each.key
    image     = local.image
    essential = true
    command   = each.value.command

    portMappings = [{ containerPort = each.value.port, protocol = "tcp" }]
    environment  = local.common_environment
    secrets      = local.common_secrets

    # Workers get a long stop timeout so SIGTERM can drain in-flight jobs.
    stopTimeout = each.key == "worker" ? 60 : 30

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.svc[each.key].name
        awslogs-region        = var.region
        awslogs-stream-prefix = each.key
      }
    }

    healthCheck = {
      command = [
        "CMD-SHELL",
        each.key == "api"
        ? "python -c \"import urllib.request as u; u.urlopen('http://localhost:8000/healthz', timeout=2)\""
        : "python -c \"import urllib.request as u; u.urlopen('http://localhost:${each.value.port}/metrics', timeout=2)\""
      ]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 20
    }
  }])
}

# One-off task that applies Alembic migrations. Run it before deploying a new version:
#   aws ecs run-task ... --task-definition <migrate family>   (see docs/DEPLOY.md)
resource "aws_ecs_task_definition" "migrate" {
  family                   = "${var.name}-${var.environment}-migrate"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "migrate"
    image     = local.image
    essential = true
    command   = ["alembic", "upgrade", "head"]
    secrets   = [{ name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.svc["migrate"].name
        awslogs-region        = var.region
        awslogs-stream-prefix = "migrate"
      }
    }
  }])
}

# --- services ---------------------------------------------------------------------------

resource "aws_ecs_service" "svc" {
  for_each = local.services

  name            = each.key
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.svc[each.key].arn
  desired_count   = each.value.desired
  launch_type     = "FARGATE"

  # Workers and the relay are scaled out and in freely: a rolling replace never drops jobs,
  # because leases are released on SIGTERM and the reaper recovers anything that is not.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.app.id]
  }

  dynamic "load_balancer" {
    for_each = each.key == "api" ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.api.arn
      container_name   = "api"
      container_port   = 8000
    }
  }

  health_check_grace_period_seconds = each.key == "api" ? 60 : null

  lifecycle {
    ignore_changes = [desired_count]
  }

  depends_on = [aws_lb_listener.http]
}

# --- worker autoscaling -----------------------------------------------------------------

resource "aws_appautoscaling_target" "worker" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.svc["worker"].name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.worker_desired_count
  max_capacity       = var.worker_max_count
}

resource "aws_appautoscaling_policy" "worker_cpu" {
  name               = "${var.name}-worker-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.worker.service_namespace
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value       = 60
    scale_in_cooldown  = 300
    scale_out_cooldown = 60

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}
