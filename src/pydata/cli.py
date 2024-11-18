import pathlib

import minio
import typer
from rich.console import Console

from pydata.data_lake import copy_to_folder, create_datalake, upload_csvs
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


@lake_app.callback()
def main(ctx: typer.Context):
    ctx.ensure_object(dict)
    client = minio.Minio("localhost:9000", access_key="minio", secret_key="minio1234", secure=False)
    ctx.obj["client"] = client


@lake_app.command()
def setup(ctx: typer.Context):
    copy_to_folder(GAME_IDS, KAGGLE_DATA_FOLDER, NOTEBOOK_DATA_DIR)

    client = ctx.obj["client"]
    create_datalake(client, [BUCKET_NAME, WAREHOUSE_NAME])


@lake_app.command()
def upload(ctx: typer.Context, folder: pathlib.Path):
    client = ctx.obj["client"]
    files = list(folder.glob("*.csv"))
    upload_csvs(client, files, BUCKET_NAME)


if __name__ == '__main__':
    app()
