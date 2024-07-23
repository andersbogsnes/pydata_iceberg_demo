import pathlib
import minio
import subprocess

DATA_DIR = pathlib.Path.cwd() / "data"
KAGGLE_DATA_FOLDER = DATA_DIR / "SteamReviews2024"


if not KAGGLE_DATA_FOLDER.exists():
    DATA_DIR.mkdir(parents=True)
