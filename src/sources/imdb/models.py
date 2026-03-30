from dataclasses import dataclass


@dataclass
class ImdbMetric:
    source_name: str
    film_title_raw: str
    rating_value: float | None
    vote_count: int | None
    popularity_rank: int | None
    source_url: str
    payload: dict

