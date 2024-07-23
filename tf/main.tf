resource "aws_s3_bucket" "datalake-bucket" {
  bucket = "pydata-copenhagen-datalake"
}

resource "aws_s3_object" "reviews-parquet" {
  bucket = aws_s3_bucket.datalake-bucket.bucket
  key    = "extract/steam_reviews.parquet"
  source = "../data/parquet/steam_reviews.parquet"

  source_hash = filemd5("../data/parquet/steam_reviews.parquet")
}

resource "aws_glue_catalog_database" "reviews-database" {
  name = "reviews"
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
          "Sid" : ""
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
    path = "${aws_s3_object.reviews-parquet.bucket}/extract"

  }
}
