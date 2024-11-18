import pathlib

from rich.progress import track

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

