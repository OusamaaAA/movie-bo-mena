from urllib.parse import quote_plus

from src.config import get_settings
from src.sources.common import BaseHttpClient


class BomClient(BaseHttpClient):
    def __init__(self) -> None:
        super().__init__(get_settings().bom_base_url)

    def search_titles(self, query: str) -> str:
        """Search BOM for titles matching query."""
        encoded = quote_plus(query.strip())
        return self.get(f"/search/?q={encoded}")

    def fetch_title_by_imdb_id(self, imdb_id: str) -> str:
        """Fetch BOM title page using IMDb tt ID (e.g., tt35927674)."""
        return self.get(f"/title/{imdb_id}/")


    def fetch_weekend_index(self, area: str, year: int) -> str:
        paths = [
            f"/weekend/by-year/?area={area}",
            f"/weekend/by-year/?area={area}&yr={year}",
        ]
        last_error: Exception | None = None
        for path in paths:
            try:
                return self.get(path)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error:
            raise last_error
        return ""

    def fetch_weekend_detail(self, area: str, weekend_code: str) -> str:
        return self.get(f"/weekend/{weekend_code}/?area={area}")

    def fetch_title_page(self, title_url_or_path: str) -> str:
        path = str(title_url_or_path or "").strip()
        if not path:
            raise ValueError("title_url_or_path is required")
        if path.startswith("http://") or path.startswith("https://"):
            if "boxofficemojo.com" not in path:
                raise ValueError("Unsupported host for BOM title page")
            path = path.split("boxofficemojo.com", 1)[1]
        if not path.startswith("/"):
            path = f"/{path}"
        return self.get(path)

    def fetch_release_page(self, release_url_or_path: str) -> str:
        path = str(release_url_or_path or "").strip()
        if not path:
            raise ValueError("release_url_or_path is required")
        if path.startswith("http://") or path.startswith("https://"):
            if "boxofficemojo.com" not in path:
                raise ValueError("Unsupported host for BOM release page")
            path = path.split("boxofficemojo.com", 1)[1]
        if not path.startswith("/"):
            path = f"/{path}"
        return self.get(path)

