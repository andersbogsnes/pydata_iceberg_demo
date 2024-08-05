variable "s3_bucket_name" {
  description = "Bucket name for the tutorial"
  type = string
  default = "pydata-copenhagen-datalake"
}

variable "aws_region" {
  description = "AWS Region to create resources in"
  type = string
  default = "eu-north-1"
}