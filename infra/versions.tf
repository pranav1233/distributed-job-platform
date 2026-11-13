terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state is strongly recommended because it contains generated passwords.
  # Create the bucket and lock table first, then uncomment.
  #
  # backend "s3" {
  #   bucket         = "YOUR-TERRAFORM-STATE-BUCKET"
  #   key            = "jobplatform/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "YOUR-TERRAFORM-LOCK-TABLE"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
