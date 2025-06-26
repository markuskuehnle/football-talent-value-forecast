"""FBref scraper for football statistics."""

import random
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class FBrefScraper:
    """Scraper for FBref football statistics."""
    
    def __init__(
        self, 
        output_dir: Optional[Path] = None,
        max_requests: int = 10,
        cooldown_seconds: int = 15 * 60,
        delay_range: tuple = (5, 10),
        current_season: str = "2425"
    ):
        """Initialize the FBref scraper.
        
        Args:
            output_dir: Directory to save scraped data. Defaults to data/raw.
            max_requests: Maximum requests before cooldown. Defaults to 10.
            cooldown_seconds: Cooldown period in seconds. Defaults to 15 minutes.
            delay_range: Range for random delays between requests (min, max). Defaults to (5, 10).
            current_season: Current season identifier. Defaults to "2425".
        """
        self.output_dir = output_dir or Path("data", "raw")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.request_counter = 0
        self.max_requests = max_requests
        self.cooldown_seconds = cooldown_seconds
        self.delay_range = delay_range
        self.current_season = current_season
        
        # Table IDs to scrape
        self.table_ids = [
            "stats_standard_12",
            "stats_shooting_12", 
            "stats_passing_12",
            "stats_passing_types_12",
            "stats_gca_12",
            "stats_defense_12",
            "stats_possession_12",
        ]
    
    def _strip_suffix(self, table_id: str, suffix: str = "_12") -> str:
        """Remove suffix from table ID to get base name."""
        return table_id[:-len(suffix)] if table_id.endswith(suffix) else table_id
    
    def _get_table_name(self, table_id: str) -> str:
        """Convert table ID to readable table name."""
        base_name = self._strip_suffix(table_id)
        if base_name == "stats_standard":
            return "stats"
        return base_name.replace("stats_", "")
    
    def _get_filename(self, table_id: str, season: str) -> str:
        """Generate filename for a table and season."""
        table_name = self._get_table_name(table_id)
        return f"df_player_{table_name}_{season}.csv"
    
    def _extract_season_from_url(self, url: str) -> str:
        """Extract season from FBref URL.
        
        Looks for season patterns in the URL path. If no season is found,
        returns the configured current season.
        """
        # Look for season patterns like /2023-2024/ or /2022-2023/
        import re
        season_pattern = r'/(\d{4}-\d{4})/'
        match = re.search(season_pattern, url)
        
        if match:
            season_range = match.group(1)
            # Convert "2023-2024" to "2324"
            start_year = season_range[:4]
            end_year = season_range[5:9]
            return f"{start_year[2:]}{end_year[2:]}"
        
        # If no season found, return current season
        return self.current_season
    
    def _calculate_estimated_time(self, num_tables: int) -> float:
        """Calculate estimated time for scraping in seconds.
        
        Args:
            num_tables: Number of tables to scrape
        """
        
        time_per_request = sum(self.delay_range) / 2 + 5  # 7.5 + 5 = 12.5 seconds
        
        # Calculate cooldown periods needed
        cooldown_periods = (num_tables - 1) // self.max_requests
        
        # Add buffer for network delays and processing overhead
        buffer_time = num_tables * 3  # 3 seconds buffer per table
        
        total_time = (num_tables * time_per_request) + (cooldown_periods * self.cooldown_seconds) + buffer_time
        return total_time
    
    def _format_time(self, seconds: float) -> str:
        """Format time in human readable format."""
        if seconds < 60:
            return f"{seconds:.0f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
    def scrape_squad_stats(self, url: str, force_overwrite: bool = False) -> Dict[str, pd.DataFrame]:
        """Scrape all squad statistics from FBref URL.
        
        Args:
            url: FBref squad URL (e.g., https://fbref.com/en/squads/dcc91a7b/Valencia-Stats)
            force_overwrite: Whether to overwrite existing files
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        season = self._extract_season_from_url(url)
        estimated_time = self._calculate_estimated_time(len(self.table_ids))
        
        print(f"Scraping FBref data for season {season}")
        print(f"URL: {url}")
        print(f"Tables to scrape: {len(self.table_ids)}")
        print(f"Estimated time: {self._format_time(estimated_time)}")
        print(f"Rate limit: {self.max_requests} requests per {self.cooldown_seconds // 60} minutes")
        print(f"Output directory: {self.output_dir}")
        print()
        print("Starting scrape...")
        print("-" * 50)
        
        scraped_data = {}
        
        for table_id in self.table_ids:
            filename = self._get_filename(table_id, season)
            filepath = self.output_dir / filename
            
            # Check if file exists and skip if not forcing overwrite
            if filepath.exists() and not force_overwrite:
                print(f"Skipping existing file: {filename}")
                continue
            
            # Rate limiting check
            if self.request_counter >= self.max_requests:
                cooldown_minutes = self.cooldown_seconds // 60
                print(f"\nRATE LIMIT REACHED!")
                print(f"   Made {self.request_counter} requests (limit: {self.max_requests})")
                print(f"   Cooling down for {cooldown_minutes} minutes to respect FBref's rate limits...")
                print(f"   This is normal - FBref allows max {self.max_requests} requests per {cooldown_minutes} minutes")
                time.sleep(self.cooldown_seconds)
                self.request_counter = 0
                print(f"Cooldown complete! Resuming scraping...\n")
            
            try:
                print(f"Fetching: {season} | {table_id}")
                df = pd.read_html(url, attrs={"id": table_id})[0]
                
                # Save to CSV
                df.to_csv(filepath, index=False)
                print(f"Saved {filename}")
                
                # Store in return dictionary
                table_name = self._get_table_name(table_id)
                scraped_data[f"df_player_{table_name}_{season}"] = df
                
                self.request_counter += 1
                
            except Exception as e:
                print(f"Failed to fetch {table_id} for {season}: {e}")
                continue
            
            # Random delay between requests
            if table_id != self.table_ids[-1]:  # Don't delay after last request
                delay = random.uniform(*self.delay_range)
                print(f"Waiting {delay:.1f} seconds...")
                time.sleep(delay)
        
        print("-" * 50)
        print(f"Scraping completed. {len(scraped_data)} tables scraped.")
        
        return scraped_data


def scrape_fbref_squad(
    url: str, 
    output_dir: Optional[Path] = None, 
    force_overwrite: bool = False,
    current_season: str = "2425",
) -> Dict[str, pd.DataFrame]:
    """Convenience function to scrape FBref squad statistics.
    
    Args:
        url: FBref squad URL
        output_dir: Directory to save scraped data
        force_overwrite: Whether to overwrite existing files
        current_season: Current season identifier
        
    Returns:
        Dictionary mapping table names to DataFrames
    """
    scraper = FBrefScraper(output_dir=output_dir, current_season=current_season)
    
    return scraper.scrape_squad_stats(url, force_overwrite) 