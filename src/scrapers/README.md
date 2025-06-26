# FBref Scraper

This module provides functionality to scrape football statistics from FBref.com for use in Jupyter notebooks.

## Features

- Scrapes 7 different stat tables from FBref squad pages
- Configurable rate limiting to avoid being blocked
- Realistic time estimation before scraping
- User confirmation with estimated time before scraping
- Saves data to CSV files with consistent naming
- Supports current and past seasons
- Configurable parameters for different use cases

## Usage in Notebooks

### Basic Usage

```python
from pathlib import Path
from src.scrapers.fbref_scraper import scrape_fbref_squad

# Scrape Valencia CF current season data
url = "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats"
result = scrape_fbref_squad(url, output_dir=Path("data", "raw"))
```

### Advanced Usage with Custom Settings

```python
from src.scrapers.fbref_scraper import FBrefScraper

# Create scraper with custom settings
scraper = FBrefScraper(
    output_dir=Path("custom_output"),
    max_requests=5,           # Lower limit for testing
    cooldown_seconds=300,     # 5 minutes cooldown
    delay_range=(2, 5),       # Faster delays
    current_season="2526"     # Custom current season
)

# Scrape with force overwrite
result = scraper.scrape_squad_stats(url, force_overwrite=True)
```

### Configuration Options

The scraper supports these configurable parameters:

- `output_dir`: Directory to save CSV files (default: `data/raw`)
- `max_requests`: Maximum requests before cooldown (default: `10`)
- `cooldown_seconds`: Cooldown period in seconds (default: `900` = 15 minutes)
- `delay_range`: Range for random delays between requests (default: `(5, 10)` seconds)
- `current_season`: Current season identifier (default: `"2425"`)

## Supported URLs

The scraper supports FBref squad URLs in these formats:

- **Current season**: `https://fbref.com/en/squads/{team_id}/{team_name}-Stats`
- **Past seasons**: `https://fbref.com/en/squads/{team_id}/{season}/{team_name}-Stats`

Examples:
- Valencia CF current: `https://fbref.com/en/squads/dcc91a7b/Valencia-Stats`
- Valencia CF 2023-24: `https://fbref.com/en/squads/dcc91a7b/2023-2024/Valencia-Stats`
- Liverpool current: `https://fbref.com/en/squads/822bd0ba/Liverpool-Stats`

## Season Detection

The scraper automatically detects seasons from URLs:
- URLs with `/2023-2024/` → season `"2324"`
- URLs with `/2022-2023/` → season `"2223"`
- URLs without season → uses configured `current_season` (default: `"2425"`)

## Output Files

The scraper generates 7 CSV files per squad/season:

- `df_player_stats_{season}.csv` - Standard statistics
- `df_player_shooting_{season}.csv` - Shooting statistics  
- `df_player_passing_{season}.csv` - Passing statistics
- `df_player_passing_types_{season}.csv` - Pass types
- `df_player_gca_{season}.csv` - Goal and shot creation
- `df_player_defense_{season}.csv` - Defensive actions
- `df_player_possession_{season}.csv` - Possession statistics

## Rate Limiting

The scraper implements configurable rate limiting to respect FBref's terms:

- Configurable maximum requests per cooldown period
- Configurable cooldown period
- Configurable random delays between requests
- Automatic cooldown when limit is reached
- Clear messaging when rate limits are hit
- User confirmation before starting

### Rate Limit Messaging

When the rate limit is reached, the scraper will display:

```
⚠️  RATE LIMIT REACHED!
   Made 10 requests (limit: 10)
   Cooling down for 15 minutes to respect FBref's rate limits...
   This is normal - FBref allows max 10 requests per 15 minutes
✅ Cooldown complete! Resuming scraping...
```

## Testing

Run tests with pytest:

```bash
python -m pytest tests/test_fbref_scraper.py -v
```

## Module Structure

```
src/scrapers/
├── __init__.py
├── fbref_scraper.py      # Main scraper class and functions
└── README.md            # This documentation
``` 