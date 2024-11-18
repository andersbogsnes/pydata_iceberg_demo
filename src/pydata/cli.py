import pathlib

import typer
from rich.console import Console

from pydata.data_lake import copy_to_folder
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
def download():
    """Download Steam Reviews from kaggle"""
    zip_file = download_kaggle_data("artermiloff/steam-games-reviews-2024")
    unzip_kaggle_data(zip_file, DATA_DIR)

@lake_app.command()
def upload():
    copy_to_folder(GAME_IDS, KAGGLE_DATA_FOLDER, NOTEBOOK_DATA_DIR.joinpath("SteamReviews2024"))

if __name__ == '__main__':
    app()