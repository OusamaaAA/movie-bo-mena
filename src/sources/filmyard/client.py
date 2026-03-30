from src.config import get_settings
from src.sources.common import BaseHttpClient


class FilmyardClient(BaseHttpClient):
    def __init__(self) -> None:
        super().__init__(get_settings().filmyard_base_url.rstrip("/"))

    def fetch_daily_egypt_page(self) -> str:
        # Current working Filmyard box office page
        return self.get("/boxoffice")

    def fetch_archive_day(self, year: int, month: int, day: int) -> str:
        # Keep a few likely archive variants because Filmyard structure may vary.
        candidates = [
            f"/boxoffice/{year}/{month:02d}/{day:02d}",
            f"/boxoffice?date={year}-{month:02d}-{day:02d}",
            f"/boxoffice/{year}-{month:02d}-{day:02d}",
        ]

        last_exc = None
        for path in candidates:
            try:
                return self.get(path)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                continue

        if last_exc:
            raise last_exc
        raise RuntimeError("No Filmyard archive URL candidate succeeded.")