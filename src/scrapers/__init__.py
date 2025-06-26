"""Football data scrapers for FBref and Transfermarkt."""

from .fbref_scraper import FBrefScraper, scrape_fbref_squad
from .transfermarkt_scraper import TransfermarktScraper, scrape_transfermarkt_team

__all__ = [
    'FBrefScraper',
    'scrape_fbref_squad', 
    'TransfermarktScraper',
    'scrape_transfermarkt_team'
]
