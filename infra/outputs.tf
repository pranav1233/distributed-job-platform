output "alb_dns_name" {
  description = "Public endpoint of the API."
  value       = aws_lb.main.dns_name
}

output "ecr_repository_url" {
  description = "Push the application image here."
  value       = aws_ecr_repository.app.repository_url
}

output "ecs_cluster" {
  value = aws_ecs_cluster.main.name
}

output "migrate_task_definition" {
  description = "Task definition family that runs database migrations."
  value       = aws_ecs_task_definition.migrate.family
}

output "private_subnet_ids" {
  description = "Subnets for one-off tasks (migrations, API key creation)."
  value       = aws_subnet.private[*].id
}

output "app_security_group_id" {
  value = aws_security_group.app.id
}

output "alarm_topic_arn" {
  value = aws_sns_topic.alarms.arn
}
