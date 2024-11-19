import pathlib
from zipfile import ZipFile

import httpx
from platformdirs import user_cache_path
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn, TransferSpeedColumn, \
    DownloadColumn, SpinnerColumn, TimeElapsedColumn, MofNCompleteColumn

from pydata.console import console
from pydata.exceptions import KaggleAPIError

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download"


def _get_cached_file(dataset: str) -> pathlib.Path:
    """
    Use platform cache directory to store and retrieve the zip file from kaggle
    :param dataset:
        name of the dataset
    :return:
        The path to the cached file
    """
    cache_folder = user_cache_path("pydata_iceberg_demo", ensure_exists=True)
    dataset_folder = cache_folder.joinpath(dataset.replace("/", "_"))
    dataset_folder.mkdir(parents=True, exist_ok=True)
    return dataset_folder.joinpath(dataset.split("/")[1]).with_suffix(".zip")


def unzip_kaggle_data(source_file: pathlib.Path, target_folder: pathlib.Path) -> None:
    """Unzip the contents of the zip file to the target folder.

    :param source_file:
        Path to the zip file
    :param target_folder:
        Path to the folder where the unzipped file will be stored
    :returns
        None
    """
    with ZipFile(source_file) as zip_file:
        all_files = zip_file.namelist()
        folder = pathlib.Path(all_files[0].split("/")[0])
        if folder.exists():
            console.print("[green]Kaggle data already extracted[/green]")
            return

        with Progress(SpinnerColumn(),
                      TextColumn("{task.description}"),
                      MofNCompleteColumn(),
                      TimeElapsedColumn(),
                      console=console) as progress:
            unzip_task = progress.add_task("Unzipping files...", total=len(all_files))
            for csv_file in all_files:
                zip_file.extract(csv_file, target_folder)
                progress.advance(unzip_task)


def download_kaggle_data(dataset: str, force: bool = False) -> pathlib.Path:
    """
    Download the dataset from Kaggle.

    :param dataset:
        name of the dataset
    :param force:
        Ignore cached files if True
    :return:
        Path to the downloaded file
    """
    cached_file = _get_cached_file(dataset)
    if cached_file.exists() and not force:
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
