import pathlib

import typer
from rich.console import Console
from rich.progress import Progress, TimeRemainingColumn, MofNCompleteColumn, BarColumn, TextColumn

from pydata.exceptions import AuthenticationError
from pydata.kaggle_data import fetch_kaggle_data

DATA_DIR = pathlib.Path.cwd() / "data"
KAGGLE_DATA_FOLDER = DATA_DIR / "SteamReviews2024"
GAME_IDS = ["10", "289070", "578080", "730"]
BUCKET_NAME = "datalake"
WAREHOUSE_NAME = "warehouse"
NOTEBOOK_DATA_DIR = pathlib.Path.cwd() / "notebooks" / "data"


app = typer.Typer()
console = Console()

progress_layout = [TextColumn("[progress.description]{task.description}"),
                   BarColumn(),
                   MofNCompleteColumn(),
                   TimeRemainingColumn()]

@app.command()
def download():
    """Download Steam Reviews from kaggle"""
    with Progress(console=console, transient=True) as progress:

        try:
            download_task = progress.add_task("Downloading dataset from kaggle...", total=None)
            fetch_kaggle_data("artermiloff/steam-games-reviews-2024",
                              output_folder=KAGGLE_DATA_FOLDER)
        except AuthenticationError as e:
            console.print(f"[red]{e}[/red]")
            return
        finally:
            progress.stop_task(download_task)
        console.print("[green]Fetched kaggle data[/green]")

if __name__ == '__main__':
    app()