# Deploying to AWS

This guide provisions the whole platform with Terraform in `infra/`: VPC, ECR, ECS Fargate
(api, worker, relay, reaper), RDS PostgreSQL, ElastiCache Redis, Amazon MQ for RabbitMQ, an
ALB, Secrets Manager, CloudWatch logs and alarms.

> `terraform apply` creates billable resources. Read the cost section first and tear the
> environment down when you are finished.

## Prerequisites

* An AWS account and credentials with permission to create the resources above
  (`aws sts get-caller-identity` should succeed).
* Terraform 1.6 or newer, AWS CLI v2, Docker with Buildx.
* Region quotas for 1 Elastic IP, 1 NAT gateway and the Fargate vCPU count you request.

## 1. Remote state (recommended)

Terraform state contains the generated database and broker passwords. Keep it encrypted.

```bash
aws s3api create-bucket --bucket YOUR-TFSTATE-BUCKET --region us-east-1
aws s3api put-bucket-versioning --bucket YOUR-TFSTATE-BUCKET --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket YOUR-TFSTATE-BUCKET \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws dynamodb create-table --table-name YOUR-TFLOCK-TABLE \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST
```

Then uncomment the `backend "s3"` block in `infra/versions.tf` and fill in the names.

## 2. Configure and plan

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # edit region, sizes, optional certificate and alarm email
terraform init
terraform plan -out plan.tfplan
```

## 3. Create the registry and push the first image

ECS services need an image to start, so create the repository first.

```bash
terraform apply -target=aws_ecr_repository.app
ECR=$(terraform output -raw ecr_repository_url)
REGION=us-east-1

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ECR%%/*}
cd ..
docker buildx build --platform linux/amd64 -t $ECR:latest --push .
cd infra
```

The task definitions use `X86_64`, hence `--platform linux/amd64` when building on Apple silicon.

## 4. Apply everything

```bash
terraform apply
```

Expect roughly 15 to 25 minutes: the Amazon MQ broker and the RDS instance are the slow parts.
Outputs include the ALB DNS name, the cluster name and the migration task family.

## 5. Run the database migration

Migrations are a one-off task, never part of container startup, so replicas cannot race on DDL.

```bash
CLUSTER=$(terraform output -raw ecs_cluster)
TASKDEF=$(terraform output -raw migrate_task_definition)
SUBNETS=$(terraform output -json private_subnet_ids | jq -r 'join(",")')
SG=$(terraform output -raw app_security_group_id)

TASK_ARN=$(aws ecs run-task --cluster $CLUSTER --launch-type FARGATE --task-definition $TASKDEF \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --query 'tasks[0].taskArn' --output text)

aws ecs wait tasks-stopped --cluster $CLUSTER --tasks $TASK_ARN
aws ecs describe-tasks --cluster $CLUSTER --tasks $TASK_ARN \
  --query 'tasks[0].containers[0].exitCode'      # must print 0
```

The services start in parallel with the first apply. Requests made before the migration finishes
fail, and the circuit breaker settles once the tables exist.

## 6. Create an API key

```bash
TASK_ARN=$(aws ecs run-task --cluster $CLUSTER --launch-type FARGATE --task-definition $TASKDEF \
  --overrides '{"containerOverrides":[{"name":"migrate","command":["python","-m","jobplatform.cli","create-api-key","--owner","demo"]}]}' \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --query 'tasks[0].taskArn' --output text)
aws ecs wait tasks-stopped --cluster $CLUSTER --tasks $TASK_ARN
```

The key is printed once to the task's log stream in `/ecs/jobplatform-dev/migrate`. Copy it, then
delete the stream so the secret does not linger in CloudWatch:

```bash
LOG_GROUP=/ecs/jobplatform-dev/migrate
STREAM=$(aws logs describe-log-streams --log-group-name $LOG_GROUP --order-by LastEventTime --descending \
  --max-items 1 --query 'logStreams[0].logStreamName' --output text)
aws logs get-log-events --log-group-name $LOG_GROUP --log-stream-name $STREAM --query 'events[].message' --output text
aws logs delete-log-stream --log-group-name $LOG_GROUP --log-stream-name $STREAM
```

## 7. Verify

```bash
ALB=$(terraform output -raw alb_dns_name)
curl -s http://$ALB/readyz
curl -s -X POST http://$ALB/v1/jobs -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"type":"io_task","payload":{"duration_ms":500}}'
```

Without `certificate_arn` the ALB serves plain HTTP on port 80. Set `certificate_arn` (an ACM
certificate in the same region) for HTTPS and an automatic HTTP to HTTPS redirect, and point your
DNS name at the ALB.

## 8. Releasing a new version

1. Build and push a new tag (`$ECR:v2`).
2. Run the migration task against the new image (task definition revisions pick up the tag when
   you apply, so apply first with the new `image_tag` and run migrations immediately after, or
   run migrations from a one-off task definition revision using the new tag).
3. `terraform apply -var image_tag=v2`.

Rolling deployments keep 100 percent of tasks healthy and replace at most the same number again.
Workers receive SIGTERM, stop consuming, drain in-flight jobs for up to 50 seconds
(`SHUTDOWN_GRACE_SECONDS`) and release the leases of anything unfinished with `attempts - 1`, so
a deploy does not consume retry budget. The deployment circuit breaker rolls back a version whose
tasks fail their health checks.

## 9. Operating

* **Logs**: `/ecs/jobplatform-<env>/<service>`, JSON. Query with CloudWatch Logs Insights, for
  example `fields @timestamp, job_id, event | filter event = "job_dead"`.
* **Alarms** (SNS topic `jobplatform-<env>-alarms`): ALB 5xx and p95 latency, API and worker CPU,
  RDS CPU, free storage and connections, Redis engine CPU, RabbitMQ CPU, jobs entering `dead`
  (log metric filter), and bursts of lease reclaims (log metric filter, a sign of crashing
  workers).
* **Metrics endpoints**: each service exposes Prometheus metrics (api on 8000, worker 9101, relay
  9102, reaper 9103). The Terraform does not deploy Prometheus or Grafana. Point Amazon Managed
  Service for Prometheus with an ADOT collector sidecar, or any Prometheus in the VPC, at those
  ports and import `deploy/grafana/dashboards/jobplatform.json`.
* **Scaling**: workers autoscale on CPU between `worker_desired_count` and `worker_max_count`.
  I/O-bound workloads are usually limited by `worker_concurrency` and database connections before
  CPU, so consider scaling on the queue depth gauge instead. Keep
  `workers x (DB_POOL_SIZE + DB_MAX_OVERFLOW)` below the RDS `max_connections`.
* **Dead jobs**: `GET /v1/dlq`, fix the cause, then `POST /v1/dlq/{id}/requeue`.

## Cost notes

These are order-of-magnitude figures from public on-demand list prices for us-east-1 with the
default variables, running 24x7. They are estimates, not measurements. Check the AWS Pricing
Calculator for your region before applying.

| Component | Default sizing | Approx. per month |
| --- | --- | --- |
| Fargate | api 2 x (0.5 vCPU, 1 GB), worker 2 x (1 vCPU, 2 GB), relay 2 and reaper 1 x (0.25 vCPU, 0.5 GB): 3.75 vCPU and 7.5 GB in total | 130 to 140 USD |
| NAT gateway | 1, plus per-GB processing | 33 USD + traffic |
| Application Load Balancer | 1 | 16 to 25 USD |
| RDS PostgreSQL | db.t4g.micro, single AZ, 20 GB gp3 | 14 to 20 USD |
| ElastiCache Redis | cache.t4g.micro, 1 node | 12 USD |
| Amazon MQ for RabbitMQ | mq.t3.micro, single instance | 20 USD |
| Secrets Manager, CloudWatch Logs and alarms, Container Insights | small | 5 to 15 USD |

That is roughly 230 to 270 USD per month, or about 8 USD per day. The NAT gateway and Fargate
dominate. Ways to cut cost for a short-lived demo: lower `api_desired_count`,
`worker_desired_count` and `relay_desired_count` to 1, and turn off Container Insights.
Multi-AZ RDS, a Multi-AZ RabbitMQ cluster and one NAT gateway per AZ roughly double the data
store and networking lines and are the production settings.

## Teardown

```bash
cd infra
# deletion protection must be off, and set the final snapshot behaviour you want
terraform apply -var db_deletion_protection=false -target=aws_db_instance.main
terraform destroy
```

Check that nothing is left behind, since the NAT gateway and load balancer bill by the hour:

```bash
aws resourcegroupstaggingapi get-resources --tag-filters Key=Project,Values=jobplatform \
  --query 'ResourceTagMappingList[].ResourceARN'
```

Also delete, if you created them, the Terraform state bucket and lock table from step 1, and any
CloudWatch log groups you chose to keep. ECR images are removed by `terraform destroy`
(`force_delete = true`), and secrets are deleted immediately (`recovery_window_in_days = 0`).
