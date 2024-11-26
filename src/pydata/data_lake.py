import pathlib
from concurrent.futures import ThreadPoolExecutor

import s3fs
from rich.progress import (track,
                           Progress,
                           TextColumn,
                           BarColumn,
                           MofNCompleteColumn,
                           TimeRemainingColumn,
                           SpinnerColumn,
                           TimeElapsedColumn)

from pydata.console import console


def copy_games_data_to_folder(game_ids: list[str],
                              input_folder: pathlib.Path,
                              output_folder: pathlib.Path) -> None:
    """Copy the data for game ids to output folder.
    :param game_ids:
        list of game ids to copy
    :param input_folder:
        folder where input data is
    :param output_folder:
        folder to copy the data to
    """
    for game_id in track(game_ids, description="Copying files to Jupyter", console=console):
        input_file = input_folder.joinpath(game_id).with_suffix(".csv")
        output_file = output_folder.joinpath(game_id).with_suffix(".csv")
        if output_file.exists():
            continue
        output_file.write_text(input_file.read_text())


def create_datalake(fs: s3fs.S3FileSystem, buckets: list[str]) -> None:
    """
    Create buckets in the datalake

    :param fs:
        an instance of the s3fs FileSystem class
    :param buckets:
        list of buckets to create
    :return: None
    """
    for bucket in track(buckets, description="Creating Minio buckets", console=console):
        if not fs.exists(bucket):
            fs.mkdir(bucket)


def upload_files(
        fs: s3fs.S3FileSystem,
        files: list[pathlib.Path],
        bucket_name: str,
        prefix: str = "extract/reviews") -> None:
    """Upload files to datalake bucket

    :param fs:
        instance of s3fs.S3FileSystem
    :param files:
        list of files to upload
    :param bucket_name:
        bucket name to upload to
    :param prefix:
        Bucket prefix to use. Defaults to extract/reviews
    """
    try:
        existing_files = fs.ls(f"{bucket_name}/{prefix}")
    except FileNotFoundError:
        existing_files = []

    if len(existing_files) > 1:
        progress = Progress("{task.description}",
                            BarColumn(),
                            MofNCompleteColumn(),
                            TimeRemainingColumn(),
                            console=console)
        upload_task = progress.add_task(f"Uploading CSV files", total=len(existing_files))
    else:
        progress = Progress(SpinnerColumn(), "{task.description}", TimeElapsedColumn(), console=console)
        upload_task = progress.add_task(f"Uploading Parquet file", total=None)

    def _process_file(file_path):
        bucket_path = f"{bucket_name}/{prefix}/{file_path.name}"
        if not bucket_path in existing_files:
            fs.put_file(file_path, f"{bucket_name}/{prefix}/{file_path.name}")
        progress.advance(upload_task)

    progress.start()
    with ThreadPoolExecutor(max_workers=10) as pool:
        pool.map(_process_file, files)
    progress.stop()


def write_parquet(input_folder: pathlib.Path, output_folder: pathlib.Path) -> pathlib.Path:
    import duckdb
    output_file = output_folder.joinpath("all_reviews.parquet")

    if output_file.exists():
        return output_file

    sql = f"""
    COPY (
    SELECT * FROM read_csv('{input_folder}/*.csv', union_by_name=true)
    WHERE recommendationid is not null
    ) TO '{output_file}' (FORMAT 'PARQUET');
    """
    with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            TimeElapsedColumn()
    ) as progress:
        task = progress.add_task("Writing parquet file...", total=None)
        duckdb.sql(sql)
        progress.stop_task(task)
    return pathlib.Path(output_folder).joinpath("all_reviews.parquet")
