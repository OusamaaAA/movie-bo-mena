from urllib.parse import quote_plus

from src.sources.common import BaseHttpClient


class LetterboxdClient(BaseHttpClient):
    def __init__(self) -> None:
        super().__init__("https://letterboxd.com")

    def search(self, query: str) -> str:
        encoded = quote_plus(query.strip())
        return self.get(f"/search/{encoded}/")

    def fetch_film_page(self, slug: str) -> str:
        return self.get(f"/film/{slug}/")
