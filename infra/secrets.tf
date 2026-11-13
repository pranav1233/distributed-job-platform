# Connection strings are stored whole so that task definitions only reference secret ARNs
# and no credential appears in the task definition or in the ECS console.

locals {
  database_url = "postgresql+asyncpg://${aws_db_instance.main.username}:${random_password.db.result}@${aws_db_instance.main.address}:${aws_db_instance.main.port}/${aws_db_instance.main.db_name}?ssl=require"
  redis_url    = "rediss://${aws_elasticache_replication_group.main.primary_endpoint_address}:6379/0"
  rabbitmq_url = "amqps://jobs:${random_password.mq.result}@${trimprefix(aws_mq_broker.main.instances[0].endpoints[0], "amqps://")}/"
}

resource "aws_secretsmanager_secret" "database_url" {
  name                    = "${var.name}/${var.environment}/database-url"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id     = aws_secretsmanager_secret.database_url.id
  secret_string = local.database_url
}

resource "aws_secretsmanager_secret" "redis_url" {
  name                    = "${var.name}/${var.environment}/redis-url"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "redis_url" {
  secret_id     = aws_secretsmanager_secret.redis_url.id
  secret_string = local.redis_url
}

resource "aws_secretsmanager_secret" "rabbitmq_url" {
  name                    = "${var.name}/${var.environment}/rabbitmq-url"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "rabbitmq_url" {
  secret_id     = aws_secretsmanager_secret.rabbitmq_url.id
  secret_string = local.rabbitmq_url
}
