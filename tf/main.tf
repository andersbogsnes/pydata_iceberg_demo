resource "aws_s3_bucket" "datalake-bucket" {
  bucket = var.s3_bucket_name
}

resource "aws_glue_catalog_database" "reviews-database" {
  name = "reviews"
}

resource "aws_glue_catalog_database" "steam-database" {
  name = "steam"
}

resource "aws_iam_role" "crawler-role" {
  name = "s3-crawler-role"
  assume_role_policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Action" : "sts:AssumeRole",
          "Principal" : {
            "Service" : "glue.amazonaws.com"
          },
          "Effect" : "Allow",
        }
      ]
    })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.crawler-role.id
}

resource "aws_iam_role_policy_attachment" "s3_policy" {
  role       = aws_iam_role.crawler-role.id
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_glue_crawler" "reviews-crawler" {
  database_name = aws_glue_catalog_database.reviews-database.name
  name          = "steam-reviews-crawler"
  role          = aws_iam_role.crawler-role.name
  s3_target {
    path = "s3://${aws_s3_bucket.datalake-bucket.bucket}/extract/reviews"
  }
  configuration = jsonencode({
    CreatePartitionIndex = true,
    Version = 1.0
  })
}
