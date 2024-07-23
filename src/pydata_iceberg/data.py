import pathlib

import httpx
import ibis
from ibis import _
import minio



def fetch_appids() -> dict:
    r = httpx.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/")
    return r.json()["applist"]["apps"]


def load_data_csv(path: pathlib.Path, name: str | None = None) -> ibis.Table:
    conn = ibis.duckdb.connect("steamreviews.ddb")
    return conn.read_csv(path / "*.csv", table_name=name, filename=True)


def transform_data(t: ibis.Table) -> ibis.Table:
    return (t.mutate(timestamp_created=t.timestamp_created.to_timestamp(),
                     timestamp_updated=t.timestamp_updated.to_timestamp(),
                     author_last_played=t.author_last_played.to_timestamp(),
                     steam_purchase=t.steam_purchase.cast(bool),
                     voted_up=t.voted_up.cast(bool)
                     )
            .mutate(year_created=_.timestamp_created.year(),
                    month_created=_.timestamp_created.month(),
                    game_id=_.filename.split("/")[-1].split(".")[0]
                    )
            .drop(_.filename)
            .filter(~t.recommendationid.isnull())
            )


def copy_to_minio():
    client = minio.Minio("http://localhost:9000")