import pathlib
from concurrent.futures import ThreadPoolExecutor

import minio
from rich.progress import track, Progress, TextColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn

from pydata.console import console


def copy_to_folder(game_ids: list[str],
                   input_folder: pathlib.Path,
                   output_folder: pathlib.Path) -> None:
    for game_id in track(game_ids, description="Copying files to Jupyter", console=console):
        input_file = input_folder.joinpath(game_id).with_suffix(".csv")
        output_file = output_folder.joinpath(game_id).with_suffix(".csv")
        if output_file.exists():
            continue
        output_file.write_text(input_file.read_text())


def create_datalake(client: minio.Minio, buckets: list[str]):
    for bucket in track(buckets, description="Creating Minio buckets", console=console):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


def upload_csvs(client: minio.Minio, files: list[pathlib.Path], bucket_name: str):
    with Progress(TextColumn("{task.description}"),
                  BarColumn(),
                  MofNCompleteColumn(),
                  TimeRemainingColumn(),
                  console=console) as progress:
        existing_files = {file.object_name.split('/')[-1].split('.')[0] for file in
                          client.list_objects("datalake", "extract/reviews", recursive=True)}
        upload_task = progress.add_task("Syncing files with minio", total=len(files))

        def _process_file(file_path):
            if file_path.stem not in existing_files:
                client.fput_object(bucket_name, f"extract/reviews/{file_path.name}", str(file_path))
            progress.advance(upload_task)

        with ThreadPoolExecutor(max_workers=10) as pool:
            pool.map(_process_file, files)
