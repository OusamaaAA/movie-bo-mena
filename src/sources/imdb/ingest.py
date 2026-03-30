from src.sources.imdb.client import ImdbClient
from src.sources.imdb.parser import parse_title_metrics


def run_imdb_daily(title_id: str, fallback_title: str) -> list:
    client = ImdbClient()
    html = client.fetch_title_page(title_id)
    metric = parse_title_metrics(html, f"{client.base_url}/title/{title_id}/", fallback_title=fallback_title)
    return [metric]

