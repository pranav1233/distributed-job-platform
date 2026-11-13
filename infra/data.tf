# --- PostgreSQL (source of truth) -----------------------------------------------------

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "main" {
  name       = "${var.name}-db"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_instance" "main" {
  identifier     = "${var.name}-${var.environment}"
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_allocated_storage * 5
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "jobs"
  username = "jobs"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = var.db_multi_az

  backup_retention_period    = 7
  auto_minor_version_upgrade = true
  deletion_protection        = var.db_deletion_protection
  skip_final_snapshot        = var.db_skip_final_snapshot
  final_snapshot_identifier  = var.db_skip_final_snapshot ? null : "${var.name}-${var.environment}-final"

  performance_insights_enabled = true
}

# --- Redis (cache and rate limiting) --------------------------------------------------

resource "aws_elasticache_subnet_group" "main" {
  name       = "${var.name}-redis"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_replication_group" "main" {
  replication_group_id = "${var.name}-${var.environment}"
  description          = "Cache and rate limiter for ${var.name}"
  engine               = "redis"
  engine_version       = "7.1"
  node_type            = var.redis_node_type
  port                 = 6379

  num_cache_clusters         = 1
  automatic_failover_enabled = false

  subnet_group_name          = aws_elasticache_subnet_group.main.name
  security_group_ids         = [aws_security_group.redis.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
}

# --- RabbitMQ (dispatch and dead letters) ---------------------------------------------

resource "random_password" "mq" {
  length  = 32
  special = false
}

resource "aws_mq_broker" "main" {
  broker_name = "${var.name}-${var.environment}"

  engine_type                = "RabbitMQ"
  engine_version             = "3.13"
  host_instance_type         = var.mq_instance_type
  deployment_mode            = "SINGLE_INSTANCE"
  auto_minor_version_upgrade = true
  publicly_accessible        = false

  subnet_ids      = [aws_subnet.private[0].id]
  security_groups = [aws_security_group.mq.id]

  user {
    username = "jobs"
    password = random_password.mq.result
  }

  logs {
    general = true
  }
}
