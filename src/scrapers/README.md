# Football Data Scrapers

This module contains scrapers for football statistics and market data from FBref and Transfermarkt.

## FBref Scraper

Scrapes detailed football performance statistics from [FBref](https://fbref.com).

### Features

- **Multi-table scraping**: Gets all 7 relevant statistical tables (standard stats, shooting, passing, etc.)
- **Dynamic season detection**: Automatically extracts season from URLs or uses current season
- **Rate limiting**: Built-in protection against FBref's rate limits (10 requests per 15 minutes)
- **Configurable delays**: Random delays between requests to avoid detection
- **File management**: Saves data to CSV files with consistent naming

### Usage

```python
from src.scrapers import scrape_fbref_squad

# Scrape Valencia CF squad statistics
url = "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats"
data = scrape_fbref_squad(url, force_overwrite=False)

# Access individual tables
stats_df = data["df_player_stats_2425"]
shooting_df = data["df_player_shooting_2425"]
```

### Advanced Usage

```python
from src.scrapers import FBrefScraper

scraper = FBrefScraper(
    output_dir=Path("custom/output"),
    max_requests=15,
    cooldown_seconds=20 * 60,
    delay_range=(3, 8),
    current_season="2324"
)

data = scraper.scrape_squad_stats(url, force_overwrite=True)
```

## Transfermarkt Scraper

Scrapes player market values and transfer data from [Transfermarkt](https://transfermarkt.com).

### Features

- **Dynamic team discovery**: Automatically finds team URLs by searching Transfermarkt
- **Multi-season scraping**: Scrapes data across multiple seasons with configurable delays
- **Market value data**: Extracts player market values, contracts, and transfer information
- **Flexible output**: Saves to Excel with optional metadata column removal
- **Rate limiting**: Random delays between requests to avoid being blocked

### Usage

```python
from src.scrapers import scrape_transfermarkt_team

# Scrape Valencia CF market values for multiple seasons
data = scrape_transfermarkt_team(
    team_name="Valencia CF",
    min_season=2020,
    max_season=2024,
    drop_metadata_columns=True
)
```

### Advanced Usage

```python
from src.scrapers import TransfermarktScraper

scraper = TransfermarktScraper(
    output_dir=Path("custom/output"),
    delay_range=(2.0, 5.0),
    headers={"User-Agent": "Custom Agent"}
)

# Scrape single season
season_data = scraper.scrape_team_season("Liverpool", 2024)

# Scrape multiple seasons
team_data = scraper.scrape_team_multiple_seasons("Real Madrid", 2020, 2024)

# Full scraping with file output
team_data = scraper.scrape_team(
    team_name="Barcelona",
    min_season=2020,
    max_season=2024,
    output_filename="barcelona_market_values.xlsx",
    drop_metadata_columns=True
)
```

## Data Sources

### FBref Data
- **Performance Statistics**: Goals, assists, passes, shots, defensive actions
- **Advanced Metrics**: Expected goals (xG), possession stats, goal-creating actions
- **Player Demographics**: Age, position, nationality, matches played

### Transfermarkt Data
- **Market Values**: Current player valuations in euros
- **Transfer Information**: Contract details, transfer fees
- **Player Demographics**: Age, position, nationality, shirt numbers

## File Structure

```
src/scrapers/
├── __init__.py              # Module exports
├── fbref_scraper.py         # FBref scraper implementation
├── transfermarkt_scraper.py # Transfermarkt scraper implementation
└── README.md               # This documentation
```

## Output Files

### FBref Output
- `df_player_stats_{season}.csv` - Standard statistics
- `df_player_shooting_{season}.csv` - Shooting statistics
- `df_player_passing_{season}.csv` - Passing statistics
- `df_player_passing_types_{season}.csv` - Pass types
- `df_player_gca_{season}.csv` - Goal-creating actions
- `df_player_defense_{season}.csv` - Defensive statistics
- `df_player_possession_{season}.csv` - Possession statistics

### Transfermarkt Output
- `{team_name}_players_{min_season}_{max_season}.xlsx` - Market value data

## Rate Limiting

Both scrapers implement rate limiting to respect website policies:

- **FBref**: 10 requests per 15 minutes with automatic cooldown
- **Transfermarkt**: Random delays (1-3 seconds default) between requests

## Error Handling

- Network errors are caught and logged
- Missing data is handled gracefully
- File existence checks prevent overwrites (unless forced)
- Invalid URLs or team names return empty DataFrames

## Testing

Run tests with pytest:

```bash
# Unit tests only
pytest tests/test_fbref_scraper.py -v
pytest tests/test_transfermarkt_scraper.py -v

# Integration tests (require network access)
pytest tests/ -m integration -v

# All tests
pytest tests/ -v
``` 