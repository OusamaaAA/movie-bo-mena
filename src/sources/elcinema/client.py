from urllib.parse import quote_plus

from src.config import get_settings
from src.sources.common import BaseHttpClient


class ElCinemaClient(BaseHttpClient):
    def __init__(self) -> None:
        super().__init__(get_settings().elcinema_base_url)

    def fetch_boxoffice_chart(self) -> str:
        return self.get("/en/boxoffice")

    def fetch_boxoffice_chart_market(self, market_code: str) -> str:
        """
        Fetch current box office chart for a specific market.
        elCinema uses both country-code and country-name URL formats.
        Tries multiple paths in order until one succeeds.
        """
        _MARKET_SLUGS: dict[str, list[str]] = {
            "EG": ["/en/boxoffice", "/en/boxoffice/EG/", "/en/boxoffice/egypt/"],
            "SA": ["/en/boxoffice/SA/", "/en/boxoffice/saudi-arabia/", "/en/boxoffice/saudi_arabia/"],
            "AE": ["/en/boxoffice/AE/", "/en/boxoffice/uae/", "/en/boxoffice/united-arab-emirates/"],
            "KW": ["/en/boxoffice/KW/", "/en/boxoffice/kuwait/"],
            "BH": ["/en/boxoffice/BH/", "/en/boxoffice/bahrain/"],
            "QA": ["/en/boxoffice/QA/", "/en/boxoffice/qatar/"],
            "OM": ["/en/boxoffice/OM/", "/en/boxoffice/oman/"],
            "JO": ["/en/boxoffice/JO/", "/en/boxoffice/jordan/"],
            "LB": ["/en/boxoffice/LB/", "/en/boxoffice/lebanon/"],
        }
        paths = _MARKET_SLUGS.get(market_code.upper(), [f"/en/boxoffice/{market_code.upper()}/"])
        last_error: Exception | None = None
        for path in paths:
            try:
                return self.get(path)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error:
            raise last_error
        return ""

    def search_works(self, query: str) -> str:
        """Search elCinema for works matching query.

        elCinema's dedicated search endpoint (/en/index/work/search/) was
        removed; the homepage ?s= parameter is the current working alternative.
        """
        encoded = quote_plus(query.strip())
        paths = [
            f"/en/?s={encoded}",           # current working homepage search
            f"/en/index/work/search/?s={encoded}",  # legacy (may return 404)
            f"/en/search/?q={encoded}",    # legacy variant
        ]
        last_error: Exception | None = None
        for path in paths:
            try:
                html = self.get(path)
                # The homepage search returns 200 but with generic content when
                # nothing matches; the dedicated work-search URLs return 404.
                # Accept any 200 response so the caller can parse work links.
                return html
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error:
            raise last_error
        return ""

    def fetch_boxoffice_chart_week(self, year: int, week: int, market_code: str = "EG") -> str:
        """Historical weekly chart. ``market_code`` EG or SA (matches :meth:`fetch_boxoffice_chart_market`)."""
        mc = (market_code or "EG").upper()
        if mc == "EG":
            paths = [
                f"/en/boxoffice?year={year}&week={week}",
                f"/en/boxoffice/egypt/{year}/week/{week}",
                f"/en/boxoffice/EG/?year={year}&week={week}",
            ]
        elif mc == "SA":
            paths = [
                f"/en/boxoffice/SA/?year={year}&week={week}",
                f"/en/boxoffice/saudi-arabia/?year={year}&week={week}",
                f"/en/boxoffice/saudi_arabia/?year={year}&week={week}",
            ]
        else:
            paths = [f"/en/boxoffice/{mc}/?year={year}&week={week}"]
        last_error: Exception | None = None
        for path in paths:
            try:
                return self.get(path)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error:
            raise last_error
        return ""

    def fetch_title_boxoffice(self, work_id: str) -> str:
        return self.get(f"/en/work/{work_id}/boxoffice")

    def fetch_work_root_en(self, work_id: str) -> str:
        return self.get(f"/en/work/{work_id}/")

    def fetch_work_root_ar(self, work_id: str) -> str:
        return self.get(f"/work/{work_id}/")

    def fetch_title_released(self, work_id: str) -> str:
        return self.get(f"/en/work/{work_id}/released")

    def fetch_title_stats(self, work_id: str) -> str:
        return self.get(f"/en/work/{work_id}/stats")

