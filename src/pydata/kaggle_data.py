import pathlib

from pydata.exceptions import AuthenticationError
import httpx
import tempfile

def fetch_kaggle_data(dataset: str, output_folder: pathlib.Path) -> None:
    with (httpx.Client(base_url="https://www.kaggle.com/api/v1/datasets/download") as client,
          tempfile.NamedTemporaryFile() as download_file):
        with client.stream("GET", f"/{dataset}") as response:
            total = int(response.headers["Content-Length"])
            yield 
