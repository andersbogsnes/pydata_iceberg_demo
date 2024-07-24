import datetime as dt
import pathlib
import random

import polars as pl

LANGUAGES = ["arabic",
             "bulgarian",
             "schine",
             "tchinese",
             "czech",
             "danish",
             "dutch",
             "english",
             "finnish",
             "french",
             "german",
             "greek",
             "hungarian",
             "indonesian",
             "italian",
             "japanese",
             "koreana",
             "norwegian",
             "polish",
             "portuguese",
             "brazilian",
             "romanian",
             "russian",
             "spanish",
             "swedish",
             "thai",
             "turkish",
             "ukrainian",
             "vietnamese"]


def generate_row() -> dict:
    timestamp_created = dt.datetime.now() + dt.timedelta(days=random.randint(1, 365))
    return {
        "recommendationid": random.randint(100_000_000, 999_999_999),
        "language": random.choice(LANGUAGES),
        "timestamp_created": int(timestamp_created.timestamp()),
        "timestamp_updated": int(timestamp_created.timestamp()),
        "voted_up": random.randint(0, 1),
        "votes_up": random.randint(0, 900),
        "votes_funny": random.randint(0, 900),
        "weighted_vote_score": random.random(),
        "comment_count": random.randint(1, 10),
        "steam_purchase": random.randint(0, 1),
        "received_for_free": random.randint(0, 1),
        "written_during_early_access": random.randint(0, 1),
        "hidden_in_steam_china": random.randint(0, 1),
        "steam_china_location": random.randint(0, 1),
        "author_steamid": random.randint(10_000_000_000_000_000, 99_999_999_999_999_999),
        "author_num_games_owned": None,
        "author_num_reviews": random.randint(0, 100),
        "author_playtime_forever": random.randint(0, 1),
        "author_playtime_last_two_weeks": random.randint(0, 1_000_000_000),
        "author_playtime_at_review": random.randint(0, 1_000_000_000),
        "author_last_played": timestamp_created - dt.timedelta(days=random.randint(1, 365)),
    }


def generate_data(num_records: int) -> list:
    return [generate_row() for _ in range(num_records)]


def create_new_file(output_path: pathlib.Path, game_id: int) -> pathlib.Path:
    num_reviews = random.randint(10, 10_000)
    df = pl.from_dicts(generate_data(num_reviews))
    output_file = output_path / f"{game_id}.csv"
    df.write_csv(output_file)
    return output_file
