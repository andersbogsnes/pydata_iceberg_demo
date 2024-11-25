import pathlib

import httpx
import s3fs
import typer
from rich.console import Console

from pydata.data_lake import copy_games_data_to_folder, create_datalake, upload_files, write_parquet
from pydata.dremio import create_dremio_sources
from pydata.kaggle_data import download_kaggle_data, unzip_kaggle_data

DATA_DIR = pathlib.Path.cwd() / "data"
KAGGLE_DATA_FOLDER = DATA_DIR / "SteamReviews2024"
GAME_IDS = ["10", "289070", "578080", "730"]
BUCKET_NAME = "datalake"
WAREHOUSE_NAME = "warehouse"
NOTEBOOK_DATA_DIR = pathlib.Path.cwd() / "notebooks" / "data"

app = typer.Typer()
console = Console()

data_app = typer.Typer()
app.add_typer(data_app, name="data")

lake_app = typer.Typer()
app.add_typer(lake_app, name="lake")


@data_app.command()
def download(force: bool = False):
    """Download Steam Reviews from kaggle"""
    zip_file = download_kaggle_data("artermiloff/steam-games-reviews-2024", force=force)
    unzip_kaggle_data(zip_file, DATA_DIR)
    output_folder = NOTEBOOK_DATA_DIR.joinpath("parquet")
    output_folder.mkdir(parents=True, exist_ok=True)
    write_parquet(KAGGLE_DATA_FOLDER, output_folder)


@lake_app.callback()
def main(ctx: typer.Context):
    ctx.ensure_object(dict)
    fs = s3fs.S3FileSystem(key="minio", secret="minio1234", endpoint_url="http://localhost:9000")
    ctx.obj["fs"] = fs


@lake_app.command()
def setup(ctx: typer.Context):
    """Set up the data lake"""
    fs = ctx.obj["fs"]
    create_datalake(fs, [BUCKET_NAME, WAREHOUSE_NAME])

    with httpx.Client(base_url="http://localhost:9047") as client:
        create_dremio_sources(client)

    upload_files(fs, list(KAGGLE_DATA_FOLDER.glob("*.csv")), BUCKET_NAME)
    upload_files(fs,
                 list(KAGGLE_DATA_FOLDER.glob("*.parquet")),
                 BUCKET_NAME,
                 prefix="extract/parquet")
    copy_games_data_to_folder(GAME_IDS, KAGGLE_DATA_FOLDER, NOTEBOOK_DATA_DIR)


if __name__ == '__main__':
    app()
