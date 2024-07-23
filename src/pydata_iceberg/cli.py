import argparse
import json
import pathlib

from pydata_iceberg.data import load_data_csv, transform_data, fetch_appids

DATASET_PATH = pathlib.Path(__file__).parents[2] / "data"

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=pathlib.Path, default=DATASET_PATH.joinpath('SteamReviews2024'),
                        help='Path to the input csv file')
    parser.add_argument('-o', '--output', type=pathlib.Path,
                        default=DATASET_PATH.joinpath('parquet'))
    return parser


def cli():
    parser = create_parser()
    args = parser.parse_args()

    print("Starting to load data...")

    input_path = args.input
    output_path = args.output / "steam_reviews.parquet"

    if not output_path.exists():
        print("Creating parquet file...")
        table = load_data_csv(input_path)
        table = transform_data(table)
        table.to_parquet(output_path)

    app_ids_path = args.output / "appids.json"

    if not app_ids_path.exists():
        print("Creating appids file...")
        app_ids = fetch_appids()
        app_ids_path.write_text(json.dumps(app_ids))

