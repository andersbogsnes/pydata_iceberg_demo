import pathlib

import httpx
import minio
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

dremio_app = typer.Typer()
app.add_typer(dremio_app, name="dremio")


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
    """Set up the data lake"""
    client = ctx.obj["client"]
    create_datalake(client, [BUCKET_NAME, WAREHOUSE_NAME])


@lake_app.command()
def upload(ctx: typer.Context, folder: pathlib.Path):
    """Upload csv files in folder"""
    client = ctx.obj["client"]
    files = list(folder.glob("*.csv"))
    upload_files(client, files, BUCKET_NAME)
    copy_games_data_to_folder(GAME_IDS, KAGGLE_DATA_FOLDER, NOTEBOOK_DATA_DIR)


@data_app.command()
def convert(
        ctx: typer.Context,
        input_folder: pathlib.Path = KAGGLE_DATA_FOLDER,
        output_folder: pathlib.Path = NOTEBOOK_DATA_DIR.joinpath("parquet")):
    """convert csv files into parquet files"""
    output_folder.mkdir(parents=True, exist_ok=True)
    output_file = write_parquet(input_folder, output_folder)
    client = ctx.obj["client"]
    upload_files(client, [output_file], BUCKET_NAME, prefix="extract/parquet")


@dremio_app.command()
def setup():
    """Set up Dremio sources"""
    with httpx.Client(base_url="http://localhost:9047") as client:
        create_dremio_sources(client)


if __name__ == '__main__':
    app()
