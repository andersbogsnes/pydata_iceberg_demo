import pathlib
import minio
import subprocess
import rich
from rich.progress import Progress

DATA_DIR = pathlib.Path.cwd() / "data"
KAGGLE_DATA_FOLDER = DATA_DIR / "SteamReviews2024"

with Progress() as progress:
    download_task = progress.add_task("Checking dataset download", total=None)
    if not KAGGLE_DATA_FOLDER.exists():
        progress.update(download_task, description="Downloading dataset from kaggle...")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run(["kaggle",
                        "datasets",
                        "download",
                        "-d",
                        "artermiloff/steam-games-reviews-2024",
                        "--unzip",
                        "--path",
                        "data"], check=True, capture_output=True)
    else:
        progress.update(download_task, description="Finished")
        progress.stop_task(download_task)

    client = minio.Minio("localhost:9000", access_key="minio", secret_key="minio1234", secure=False)

    if not client.bucket_exists("datalake"):
        client.make_bucket("datalake")

    files = list(KAGGLE_DATA_FOLDER.glob("*.csv"))
    existing_files = client.list_objects("datalake", "extract", recursive=True)
    upload_task = progress.add_task("Uploading dataset", total=len(files))
    for file in files:
        client.fput_object('datalake', f"extract/{file.name}", str(file))
        progress.advance(upload_task)
