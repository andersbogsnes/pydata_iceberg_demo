# Pydata Copenhagen Apache Iceberg Demo

This tutorial will introduce you to the Apache Iceberg table format and walk through how we can use it from a Python
point-of-view to work with large amounts of data stored in object storage.

# Prerequisites

To be able to follow along, make sure you've cloned the repository locally.



You will need to have [Docker](https://docs.docker.com/get-docker/) and 
[Docker Compose](https://docs.docker.com/compose/install/) installed to run the required images.

You will need a [Kaggle](https://kaggle.com) account to download the dataset. After logging in,
follow the instructions [here](https://github.com/Kaggle/kaggle-api/blob/main/docs/README.md) to
set up your API keys. 

Additionally, if you want to follow the AWS examples, you will need an AWS account and have 
created a set of access keys and placed them in the .env file. 
You can optionally use the terraform files found in the `tf` folder to create the 
necessary infrastructure


# Start the notebooks

Start the docker compose containers - this might take a while the first time as you have to 
download the various images

```bash
docker compose up -d
```

## Handy dandy links

### Jupyterlab
http://localhost:8080


### Minio Console
http://localhost:9001

- Username: minio
- Password: minio1234

### Dremio
http://localhost:9047

- Username: dremio
- Password: dremio123

## Python Environment
While the notebooks themselves run within a dockerized environment, 
you can choose to use the `prepare_data.py` script to fetch and upload the data to Minio 
and the notebook data directory

With your favourite virtualenv manager, create a virtualenv and install the requirements.txt
```bash
pip install -r requirements.txt
```

```bash
python prepare_data.py
```

## Dataset
We are using the Steam Review dataset that can be found on 
[Kaggle](https://www.kaggle.com/datasets/artermiloff/steam-games-reviews-2024/data). 
This dataset is around 13Gb and contains 80,000 game reviews scraped from a 
[Steam API endpoint](https://partner.steamgames.com/doc/store/getreviews)

## Manual steps

If you want to download the data manually, you can run the following kaggle CLI command, or 
download it from https://www.kaggle.com/datasets/artermiloff/steam-games-reviews-2024/data.

```bash
kaggle datasets download -d artermiloff/steam-games-reviews-2024 --unzip --path data
```

After unzipping, upload the csv files you'd like to play with into the `notebooks/data` folder 
as well as to the `Minio` server in a bucket named `datalake`. 
To be able to follow along, you will need at least the following files:
- `10.csv`
- `289070.csv`
- `578080.csv`
- `730.csv`

In addition, you will need to create a bucket named `warehouse` in the Minio server via the UI

## Optional: AWS Setup

If you want to follow along with the AWS part, make sure you have the AWS CLI installed and 
configured

### Creating the AWS infrastructure with terraform
```bash
terraform -chdir=tf init
terraform -chdir=tf apply
````

### Upload data to S3 bucket

To upload the full dataset to an S3 bucket will take some time due to the large dataset.
The easiest way to ensure that all the data is uploaded is to use the `aws s3 sync` command

```bash
aws s3 sync data/SteamReviews2024 s3://pydata-copenhagen-datalake/extract/reviews
```

