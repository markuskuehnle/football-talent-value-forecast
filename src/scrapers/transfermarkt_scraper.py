import random
import re
import ssl
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup


class TransfermarktScraper:
    """Scraper for Transfermarkt player market values and transfer data."""
    
    def __init__(
        self,
        output_dir: Optional[Path] = None,
        delay_range: tuple = (1.0, 3.0),
        headers: Optional[Dict[str, str]] = None
    ):
        """Initialize the Transfermarkt scraper.
        
        Args:
            output_dir: Directory to save scraped data. Defaults to data/raw.
            delay_range: Range for random delays between requests (min, max). Defaults to (1.0, 3.0).
            headers: HTTP headers for requests. Defaults to standard browser headers.
        """
        self.output_dir = output_dir or Path("data", "raw")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.delay_range = delay_range
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }
    
    def get_transfermarkt_club_url(self, club_name: str, country: str = 'COM') -> Optional[str]:
        """Get Transfermarkt club URL by searching for the club name.
        
        Args:
            club_name: Name of the club to search for
            country: Country code for the search (DE, EN, etc.)
            
        Returns:
            Club URL if found, None otherwise
        """
        # Format search query
        base_search_url = f'https://www.transfermarkt.{country.lower()}/schnellsuche/ergebnis/schnellsuche'
        query = {'query': club_name}
        search_url = f"{base_search_url}?{urllib.parse.urlencode(query)}"

        try:
            response = requests.get(search_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find club link in search results
            club_links = soup.select('a[href*="/startseite/verein/"]')
            for link in club_links:
                href = link.get('href', '')
                if '/startseite/verein/' in href:
                    return urllib.parse.urljoin(f"https://www.transfermarkt.com", href)

            return None  # If no match found
            
        except Exception as e:
            print(f"Error searching for club {club_name}: {e}")
            return None
    
    @staticmethod
    def extract_slug_and_id(club_url: str) -> Optional[Dict[str, str]]:
        team_id_match = re.search(r'/verein/(\d+)', club_url)
        team_slug_match = re.search(r'transfermarkt\.com/([^/]+)/startseite', club_url)
        if team_id_match and team_slug_match:
            return {'team_id': team_id_match.group(1), 'team_slug': team_slug_match.group(1)}
        return None
    
    def _random_delay(self, min_seconds: Optional[float] = None, max_seconds: Optional[float] = None) -> None:
        """Add random delay to avoid being blocked.
        
        Args:
            min_seconds: Minimum delay in seconds. Defaults to self.delay_range[0].
            max_seconds: Maximum delay in seconds. Defaults to self.delay_range[1].
        """
        min_delay = min_seconds or self.delay_range[0]
        max_delay = max_seconds or self.delay_range[1]
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def _extract_age_from_cell(self, age_text: str) -> Optional[int]:
        """Extract age from the date of birth/age cell.
        
        Args:
            age_text: Text containing age information
            
        Returns:
            Extracted age as integer, or None if not found
        """
        if pd.isna(age_text) or age_text == '':
            return None
        
        # Look for age pattern like "(24)" or "24"
        age_match = re.search(r'\((\d+)\)', str(age_text))
        if age_match:
            return int(age_match.group(1))
        
        return None
    
    def _extract_nationality_from_cell(self, nat_cell) -> str:
        """Extract nationality from the nationality cell using flag alt attribute.
        
        Args:
            nat_cell: BeautifulSoup element containing nationality information
            
        Returns:
            Extracted nationality as string
        """
        if pd.isna(nat_cell) or nat_cell == '':
            return 'Unknown'
        
        # Look for flaggenrahmen img tags
        if hasattr(nat_cell, 'find'):
            flag_imgs = nat_cell.find_all('img', {'class': 'flaggenrahmen'})
            if flag_imgs:
                # Get the first nationality (primary)
                alt_text = flag_imgs[0].get('alt', '')
                if alt_text:
                    return alt_text
        
        return 'Unknown'
    
    def scrape_team_season(self, team_name: str, season: int, team_slug: str = None, team_id: str = None) -> pd.DataFrame:
        """Scrape team player data for a specific season from English Transfermarkt.
        
        Args:
            team_name: Name of the team to scrape
            season: Season year to scrape
            
        Returns:
            DataFrame containing player data for the season
        """
        if team_slug and team_id:
            url = f"https://www.transfermarkt.com/{team_slug}/kader/verein/{team_id}/saison_id/{season}/plus/1"
        else:
            url = None
        if not url:
            print(f"Could not generate URL for {team_name} season {season}")
            return pd.DataFrame()
        
        try:
            ssl._create_default_https_context = ssl._create_unverified_context
            req = Request(url, headers=self.headers)
            html = urlopen(req)
            
            # Parse with BeautifulSoup to get the exact table structure
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find the squad table using the exact CSS selector
            squad_table = soup.select_one('div.responsive-table table.items')
            if not squad_table:
                print(f"No squad table found for {team_name} season {season}")
                return pd.DataFrame()
            
            # Find all player rows using the exact CSS selector
            player_rows = squad_table.select('tbody > tr')
            
            # Process the data
            processed_data = []
            
            for row in player_rows:
                try:
                    # Extract shirt number
                    number_cell = row.select_one('td.rn_nummer')
                    shirt_number = number_cell.text.strip() if number_cell else None
                    
                    # Extract player name and profile link
                    name_link = row.select_one('td.hauptlink a')
                    player_name = name_link.text.strip() if name_link else 'Unknown'
                    profile_url = name_link.get('href') if name_link else None
                    
                    # Extract player image
                    player_img = row.select_one('td.hauptlink img')
                    player_photo = None
                    if player_img:
                        player_photo = player_img.get('data-src') or player_img.get('src')
                    
                    # Extract position from inline table
                    posrela_cell = row.select_one('td.posrela')
                    position = 'Unknown Position'
                    if posrela_cell:
                        inline_table = posrela_cell.select_one('table.inline-table')
                        if inline_table:
                            position_rows = inline_table.select('tr')
                            if len(position_rows) > 1:
                                position_cell = position_rows[1].select_one('td')
                                if position_cell:
                                    position = position_cell.text.strip()
                    
                    # Extract age from zentriert cells (find the one with age pattern)
                    zentriert_cells = row.select('td.zentriert')
                    age = None
                    for cell in zentriert_cells:
                        if re.search(r'\(\d+\)', cell.text):
                            age = self._extract_age_from_cell(cell.text)
                            break
                    
                    # Extract nationality from flag images
                    nationality = 'Unknown'
                    flag_imgs = row.select('td img.flaggenrahmen')
                    if flag_imgs:
                        nationality = flag_imgs[0].get('alt', 'Unknown')
                    
                    # Extract market value
                    market_value_cell = row.select_one('td.rechts')
                    market_value = '€0'
                    if market_value_cell:
                        market_value_link = market_value_cell.select_one('a')
                        if market_value_link:
                            market_value = market_value_link.text.strip()
                    
                    # Extract contract (if available)
                    contract = None
                    # Look for contract info in the last zentriert cell or specific contract column
                    contract_cells = row.select('td.zentriert')
                    if len(contract_cells) > 2:  # Assuming contract might be in later zentriert cells
                        for cell in contract_cells[-2:]:  # Check last two zentriert cells
                            cell_text = cell.text.strip()
                            if cell_text and not re.search(r'\(\d+\)', cell_text) and not cell_text.isdigit():
                                contract = cell_text
                                break
                    
                    # Create player record
                    player_record = {
                        'Player': [player_name, position],
                        'Age': age,
                        'Current club': team_name,
                        'Market value': market_value,
                        'Nat.': nationality,
                        'Season': season,
                        'Contract': contract,
                        'Shirt Number': shirt_number,
                        'Profile URL': profile_url,
                        'Photo URL': player_photo
                    }
                    
                    processed_data.append(player_record)
                    
                except Exception as e:
                    print(f"Error processing player row: {str(e)}")
                    continue
            
            result_df = pd.DataFrame(processed_data)
            print(f"Successfully scraped {len(result_df)} players for {team_name} season {season}")
            return result_df
            
        except Exception as e:
            print(f"Error scraping {team_name} season {season}: {str(e)}")
            return pd.DataFrame()
    
    def scrape_team_multiple_seasons(self, team_name: str, min_season: int, max_season: int, team_slug: str = None, team_id: str = None) -> pd.DataFrame:
        """Scrape team data for multiple seasons with random delays.
        
        Args:
            team_name: Name of the team to scrape
            min_season: Starting season year
            max_season: Ending season year
            
        Returns:
            DataFrame containing player data for all seasons
        """
        all_data = []
        
        for season in range(min_season, max_season + 1):
            print(f"Scraping {team_name} season {season}...")
            season_data = self.scrape_team_season(team_name, season, team_slug=team_slug, team_id=team_id)
            
            if not season_data.empty:
                all_data.append(season_data)
            
            # Add random delay to avoid being blocked
            if season < max_season:  # Don't delay after the last season
                delay = random.uniform(*self.delay_range)
                print(f"Waiting {delay:.1f} seconds before next request...")
                self._random_delay()
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data.index = range(len(combined_data.index))
            return combined_data
        else:
            return pd.DataFrame()
    
    def scrape_team(self, team_name: str, min_season: int, max_season: int, 
                   output_filename: Optional[str] = None, 
                   drop_metadata_columns: bool = True) -> pd.DataFrame:
        """Scrape any team's data across multiple seasons from English Transfermarkt.
        
        Args:
            team_name: Name of the team to scrape
            min_season: Starting season year
            max_season: Ending season year
            output_filename: Optional filename to save the data
            drop_metadata_columns: Whether to drop Shirt Number, Photo URL, and Profile URL columns
            
        Returns:
            DataFrame containing player data for all seasons
        """
        print(f"Starting to scrape {team_name} players from season {min_season} to {max_season}")
        print("-" * 60)
        
        # Lookup club URL and extract slug/id ONCE
        club_url = self.get_transfermarkt_club_url(team_name, country='COM')
        if not club_url:
            print(f"Could not find Transfermarkt URL for team: {team_name}")
            return pd.DataFrame()
        slug_id = self.extract_slug_and_id(club_url)
        if not slug_id:
            print(f"Could not extract team slug or ID from URL: {club_url}")
            return pd.DataFrame()
        team_slug = slug_id['team_slug']
        team_id = slug_id['team_id']
        team_data = self.scrape_team_multiple_seasons(team_name, min_season, max_season, team_slug=team_slug, team_id=team_id)
        
        if not team_data.empty:
            print(f"\nScraped {len(team_data)} player records")
            
            # Drop metadata columns if requested
            if drop_metadata_columns:
                columns_to_drop = ['Shirt Number', 'Photo URL', 'Profile URL']
                team_data = team_data.drop(columns=[col for col in columns_to_drop if col in team_data.columns])
                print(f"Dropped metadata columns: {columns_to_drop}")
            
            # Generate output filename if not provided
            if output_filename is None:
                output_filename = f"{team_name.lower().replace(' ', '_')}_players_{min_season}_{max_season}.xlsx"
            
            # Save to Excel
            filepath = self.output_dir / output_filename
            team_data.to_excel(filepath, index=False)
            print(f"\nData saved to {filepath}")
            print(f"Total records: {len(team_data)}")
            
            return team_data
        else:
            print("No data was scraped. Please check the team name and season range.")
            return pd.DataFrame()


def scrape_transfermarkt_team(
    team_name: str, 
    min_season: int, 
    max_season: int,
    output_dir: Optional[Path] = None,
    output_filename: Optional[str] = None,
    drop_metadata_columns: bool = True
) -> pd.DataFrame:
    """Convenience function to scrape Transfermarkt team data.
    
    Args:
        team_name: Name of the team to scrape
        min_season: Starting season year
        max_season: Ending season year
        output_dir: Directory to save scraped data
        output_filename: Optional filename to save the data
        drop_metadata_columns: Whether to drop metadata columns
        
    Returns:
        DataFrame containing player data for all seasons
    """
    scraper = TransfermarktScraper(output_dir=output_dir)
    return scraper.scrape_team(
        team_name=team_name,
        min_season=min_season,
        max_season=max_season,
        output_filename=output_filename,
        drop_metadata_columns=drop_metadata_columns
    ) 