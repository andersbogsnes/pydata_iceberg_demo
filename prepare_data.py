import pathlib
import shutil
import subprocess

import httpx
import minio
from rich.console import Console
from rich.progress import Progress, MofNCompleteColumn, BarColumn, TextColumn, TimeRemainingColumn

DATA_DIR = pathlib.Path.cwd() / "data"
KAGGLE_DATA_FOLDER = DATA_DIR / "SteamReviews2024"
GAME_IDS = ["10", "289070", "578080", "730"]
BUCKET_NAME = "datalake"
WAREHOUSE_NAME = "warehouse"
NOTEBOOK_DATA_DIR = pathlib.Path.cwd() / "notebooks" / "data"

console = Console()
progress_layout = [TextColumn("[progress.description]{task.description}"),
                   BarColumn(),
                   MofNCompleteColumn(),
                   TimeRemainingColumn()]


def download_kaggle_data(data_folder: pathlib.Path):
    with Progress(*progress_layout, console=console) as progress:
        download_task = progress.add_task("Downloading dataset from kaggle...", total=None)
        subprocess.run(["kaggle",
                        "datasets",
                        "download",
                        "-d",
                        "artermiloff/steam-games-reviews-2024",
                        "--unzip",
                        "--path",
                        data_folder.absolute()], check=True, capture_output=True)
        progress.stop_task(download_task)


def upload_csvs(client: minio.Minio, csv_files: list[pathlib.Path]):
    with Progress(*progress_layout, console=console) as progress:
        existing_files = {file.object_name.split('/')[-1].split('.')[0] for file in
                          client.list_objects("datalake", "extract/reviews", recursive=True)}
        upload_task = progress.add_task("Syncing files with minio", total=len(csv_files))
        for file in csv_files:
            if file.stem in existing_files:
                continue
            else:
                client.fput_object(BUCKET_NAME, f"extract/reviews/{file.name}", str(file))

            shutil.copy(file, NOTEBOOK_DATA_DIR)
            progress.advance(upload_task)


def create_dremio_sources(client: httpx.Client):
    token = client.post("/apiv2/login", json={"userName": "dremio", "password": "dremio123"}).json()["token"]
    client.headers.update({"Authorization": f"_dremio{token}"})

    # Create Nessie source
    payload = {
        "entityType": "source",
        "name": "Nessie",
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": "http://nessie:19120/api/v2",
            "nessieAuthType": "NONE",
            "awsAccessKey": "minio",
            "awsAccessSecret": "minio1234",
            "awsRootPath": "/warehouse",
            "propertyList": [{
                "fs.s3a.endpoint": "minio:9000",
                "fs.s3a.path.style.access": "true",
                "dremio.s3.compat": "true"
            }]
        }
    }
    resp = client.post("/api/v3/catalog", json=payload)
    print(resp.json())

    payload = {
        "entityType": "source",
        "name": "Extract",
        "type": "S3",
        "config": {
            "accessKey": "minio",
            "accessSecret": "minio1234",
            "awsRootPath": "/datalake",
            "secure": "false",
            "compatibilityMode": "true",
            "propertyList": [{
                "fs.s3a.endpoint": "minio:9000",
                "fs.s3a.path.style.access": "true",
            }]
        }
    }
    resp = client.post("/api/v3/catalog", json=payload)
    print(resp.json())


if __name__ == '__main__':
    if not KAGGLE_DATA_FOLDER.exists():
        DATA_DIR.mkdir(parents=True)
        download_kaggle_data(DATA_DIR)
    else:
        console.print("Kaggle data already exists, skipping...")

    client = minio.Minio("localhost:9000", access_key="minio", secret_key="minio1234", secure=False)

    for bucket in [BUCKET_NAME, WAREHOUSE_NAME]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    csv_files = [data_file for data_file in KAGGLE_DATA_FOLDER.glob("*.csv") if data_file.stem in GAME_IDS]
    upload_csvs(client, csv_files)
