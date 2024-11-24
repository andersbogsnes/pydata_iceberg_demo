import pathlib
from concurrent.futures import ThreadPoolExecutor

import minio
from rich.progress import track, Progress, TextColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn, \
    SpinnerColumn, TimeElapsedColumn

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


def create_datalake(client: minio.Minio, buckets: list[str]) -> None:
    """
    Create buckets in the datalake
    :param client:
        instance of minio client
    :param buckets:
        list of buckets to create
    :return: None
    """
    for bucket in track(buckets, description="Creating Minio buckets", console=console):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


def upload_files(client: minio.Minio, files: list[pathlib.Path], bucket_name: str, prefix: str = "extract/reviews") -> None:
    """Upload files to datalake bucket
    :param client:
        an instance of minio client
    :param files:
        list of files to upload
    :param bucket_name:
        bucket name to upload to
    :param prefix:
        Bucket prefix to use. Defaults to extract/reviews
    """
    with Progress(TextColumn("{task.description}"),
                  BarColumn(),
                  MofNCompleteColumn(),
                  TimeRemainingColumn(),
                  console=console) as progress:
        existing_files = {file.object_name.split('/')[-1].split('.')[0] for file in
                          client.list_objects(bucket_name, prefix, recursive=True)}
        upload_task = progress.add_task("Syncing files with minio", total=len(files))

        def _process_file(file_path):
            if file_path.stem not in existing_files:
                client.fput_object(bucket_name, f"{prefix}/{file_path.name}", str(file_path))
            progress.advance(upload_task)

        with ThreadPoolExecutor(max_workers=10) as pool:
            pool.map(_process_file, files)

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