import pathlib
import shutil
from zipfile import ZipFile

import httpx
from platformdirs import user_cache_path
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn, TransferSpeedColumn, \
    DownloadColumn, SpinnerColumn, TimeElapsedColumn

from pydata.console import console
from pydata.exceptions import KaggleAPIError

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download"

def _get_cached_file(dataset: str) -> pathlib.Path:
    cache_folder = user_cache_path("pydata_iceberg_demo", ensure_exists=True)
    dataset_folder = cache_folder.joinpath(dataset.replace("/", "_"))
    dataset_folder.mkdir(parents=True, exist_ok=True)
    return dataset_folder.joinpath(dataset.split("/")[1]).with_suffix(".zip")

def unzip_kaggle_data(source_file: pathlib.Path, target_folder: pathlib.Path):
    with ZipFile(source_file) as zip_file:
        folder = pathlib.Path(zip_file.namelist()[0].split("/")[0])
        if folder.exists():
            console.print("[green]Kaggle data already extracted[/green]")
            return

        with Progress(SpinnerColumn(),
                       TextColumn("{"),
                       TimeElapsedColumn(),

                       console=console) as progress:
             progress.add_task("")
             zip_file.extractall(target_folder)


def download_kaggle_data(dataset: str):
    cached_file = _get_cached_file(dataset)
    if cached_file.exists():
        console.print("[green]Kaggle data already downloaded, using cache...[/green]")
        return cached_file

    progress = Progress(TextColumn("{task.description}{task.percentage:>3.0f}%"),
                        BarColumn(bar_width=None),
                        DownloadColumn(),
                        TimeRemainingColumn(),
                        TransferSpeedColumn(),
                        console=console)

    with httpx.stream("GET",
                      f"{KAGGLE_URL}/{dataset}",
                      follow_redirects=True) as response, progress:
        if response.is_error:
            raise KaggleAPIError(f"Error downloading from Kaggle: {response.text}")
        total_bytes = int(response.headers["Content-Length"])
        download_task = progress.add_task("Downloading dataset from kaggle...",
                                          total=total_bytes)
        with cached_file.open(mode="wb") as f:
            for chunk in response.iter_bytes():
                f.write(chunk)
                progress.update(download_task, completed=response.num_bytes_downloaded)
        return cached_file


