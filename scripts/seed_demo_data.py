from datetime import date
from decimal import Decimal

from src.db import session_scope
from src.models import Film, FilmAlias, MarketingInput, OutcomeTarget, RatingsMetric
from src.services.text_utils import normalize_title


def main() -> None:
    with session_scope() as session:
        film = Film(
            canonical_title="Dune: Part Two",
            normalized_title=normalize_title("Dune: Part Two"),
            release_year=2024,
        )
        session.add(film)
        session.flush()
        session.add(
            FilmAlias(
                film_id=film.id,
                alias_text="كثيب الجزء الثاني",
                normalized_alias=normalize_title("كثيب الجزء الثاني"),
                alias_language="ar",
                alias_type="localized",
                source="seed",
            )
        )
        session.add(
            RatingsMetric(
                film_id=film.id,
                source_name="IMDb",
                rating_value=Decimal("8.6"),
                vote_count=355000,
                popularity_rank=18,
                metric_date=date.today(),
                raw_payload_json={"seed": True},
            )
        )
        session.add(
            MarketingInput(
                film_id=film.id,
                market_code="EG",
                spend_local=Decimal("2200000"),
                spend_currency="EGP",
            )
        )
        session.add(
            OutcomeTarget(
                film_id=film.id,
                market_code="EG",
                target_label="first_watch_target",
                target_value=Decimal("180000"),
                target_unit="admissions",
            )
        )
    print("Demo data inserted.")


if __name__ == "__main__":
    main()

