import json
from urllib.parse import quote

import httpx

from src.config import get_settings
from src.sources.common import BaseHttpClient


class ImdbClient(BaseHttpClient):
    def __init__(self) -> None:
        super().__init__(get_settings().imdb_base_url)

    def fetch_title_page(self, title_id: str) -> str:
        return self.get(f"/title/{title_id}/")

    def search_suggestions(self, query: str) -> list[dict]:
        """Use IMDb's suggestion API to find movies by title.

        Returns list of dicts with keys: imdb_id, title, year, type.
        Only returns movie/feature results (filters out people, TV, etc.).
        """
        q = query.strip().lower()
        if not q:
            return []
        encoded = quote(q)
        # IMDb suggestion endpoint uses first char as path segment
        first_char = q[0] if q[0].isalpha() else "x"
        url = f"https://v3.sg.media-imdb.com/suggestion/{first_char}/{encoded}.json"
        try:
            resp = httpx.get(url, timeout=10.0, headers={"User-Agent": "MENA-BoxOffice-Intelligence/0.1"})
            resp.raise_for_status()
            data = resp.json()
        except Exception:  # noqa: BLE001
            return []

        out: list[dict] = []
        for item in data.get("d") or []:
            item_id = item.get("id", "")
            if not item_id.startswith("tt"):
                continue  # Skip people (nm*) and other non-title entries
            qid = item.get("qid", "")
            # Accept movies, features, TV movies — skip shorts, series, episodes
            if qid and qid not in ("movie", "tvMovie"):
                continue
            out.append({
                "imdb_id": item_id,
                "title": item.get("l", ""),
                "year": item.get("y"),
                "type": qid,
                "rank": item.get("rank"),
                "cast": item.get("s", ""),
            })
        return out

