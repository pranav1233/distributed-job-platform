resource "aws_sns_topic" "alarms" {
  name = "${var.name}-${var.environment}-alarms"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

locals {
  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "alb_5xx" {
  alarm_name          = "${var.name}-${var.environment}-alb-5xx"
  alarm_description   = "The API is returning server errors"
  namespace           = "AWS/ApplicationELB"
  metric_name         = "HTTPCode_Target_5XX_Count"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  threshold           = 10
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  dimensions = {
    LoadBalancer = aws_lb.main.arn_suffix
  }
}

resource "aws_cloudwatch_metric_alarm" "alb_latency" {
  alarm_name          = "${var.name}-${var.environment}-alb-p95-latency"
  alarm_description   = "API p95 latency above one second"
  namespace           = "AWS/ApplicationELB"
  metric_name         = "TargetResponseTime"
  extended_statistic  = "p95"
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  threshold           = 1
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  dimensions = {
    LoadBalancer = aws_lb.main.arn_suffix
  }
}

resource "aws_cloudwatch_metric_alarm" "ecs_cpu" {
  for_each = toset(["api", "worker"])

  alarm_name          = "${var.name}-${var.environment}-${each.key}-cpu"
  alarm_description   = "${each.key} service CPU above 85 percent"
  namespace           = "AWS/ECS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 60
  evaluation_periods  = 5
  threshold           = 85
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = local.alarm_actions

  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = each.key
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${var.name}-${var.environment}-rds-cpu"
  alarm_description   = "PostgreSQL CPU above 80 percent"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = local.alarm_actions

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_storage" {
  alarm_name          = "${var.name}-${var.environment}-rds-free-storage"
  alarm_description   = "PostgreSQL free storage below 2 GiB"
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 2147483648
  comparison_operator = "LessThanThreshold"
  alarm_actions       = local.alarm_actions

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "rds_connections" {
  alarm_name          = "${var.name}-${var.environment}-rds-connections"
  alarm_description   = "PostgreSQL connection count is high (check pool sizes against max_connections)"
  namespace           = "AWS/RDS"
  metric_name         = "DatabaseConnections"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 70
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = local.alarm_actions

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.identifier
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu" {
  alarm_name          = "${var.name}-${var.environment}-redis-cpu"
  alarm_description   = "Redis engine CPU above 80 percent"
  namespace           = "AWS/ElastiCache"
  metric_name         = "EngineCPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = local.alarm_actions

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.main.replication_group_id}-001"
  }
}

resource "aws_cloudwatch_metric_alarm" "mq_cpu" {
  alarm_name          = "${var.name}-${var.environment}-mq-cpu"
  alarm_description   = "RabbitMQ broker CPU above 80 percent"
  namespace           = "AWS/AmazonMQ"
  metric_name         = "SystemCpuUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 80
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = local.alarm_actions

  dimensions = {
    Broker = aws_mq_broker.main.broker_name
  }
}

# --- application level signals derived from structured logs ---------------------------

resource "aws_cloudwatch_log_metric_filter" "job_dead" {
  name           = "${var.name}-${var.environment}-job-dead"
  log_group_name = aws_cloudwatch_log_group.svc["worker"].name
  pattern        = "{ $.event = \"job_dead\" }"

  metric_transformation {
    name      = "JobsDead"
    namespace = "${var.name}/${var.environment}"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "dead_jobs" {
  alarm_name          = "${var.name}-${var.environment}-dead-jobs"
  alarm_description   = "Jobs are landing in the dead-letter state"
  namespace           = "${var.name}/${var.environment}"
  metric_name         = "JobsDead"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 5
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_log_metric_filter" "lease_reclaimed" {
  name           = "${var.name}-${var.environment}-lease-reclaimed"
  log_group_name = aws_cloudwatch_log_group.svc["reaper"].name
  pattern        = "{ $.event = \"lease_reclaimed\" }"

  metric_transformation {
    name      = "LeaseReclaims"
    namespace = "${var.name}/${var.environment}"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "lease_reclaims" {
  alarm_name          = "${var.name}-${var.environment}-lease-reclaims"
  alarm_description   = "Workers are crashing or hanging: many expired leases were reclaimed"
  namespace           = "${var.name}/${var.environment}"
  metric_name         = "LeaseReclaims"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 20
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}
