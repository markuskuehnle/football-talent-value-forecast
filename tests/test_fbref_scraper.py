"""Tests for FBref scraper."""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd

from src.scrapers.fbref_scraper import FBrefScraper


class TestFBrefScraper:
    """Test FBref scraper functionality."""
    
    @pytest.fixture(autouse=True)
    def setup_test_output_dir(self):
        """Create test output directory for all tests."""
        test_output_dir = Path("tests", "tmp", "scraper_output")
        test_output_dir.mkdir(parents=True, exist_ok=True)
        yield test_output_dir
        # Cleanup could be added here if needed
    
    def test_extract_season_from_url_current_season(self):
        """Test season extraction from current season URL."""
        scraper = FBrefScraper()
        url = "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats"
        
        season = scraper._extract_season_from_url(url)
        
        assert season == "2425"
    
    def test_extract_season_from_url_past_season(self):
        """Test season extraction from past season URL."""
        scraper = FBrefScraper()
        url = "https://fbref.com/en/squads/dcc91a7b/2023-2024/Valencia-Stats"
        
        season = scraper._extract_season_from_url(url)
        
        assert season == "2324"
    
    def test_extract_season_from_url_custom_current_season(self):
        """Test season extraction with custom current season."""
        scraper = FBrefScraper(current_season="2425")
        url = "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats"
        
        season = scraper._extract_season_from_url(url)
        
        assert season == "2425"
    
    def test_get_table_name_standard_stats(self):
        """Test table name extraction for standard stats."""
        scraper = FBrefScraper()
        
        table_name = scraper._get_table_name("stats_standard_12")
        
        assert table_name == "stats"
    
    def test_get_table_name_shooting_stats(self):
        """Test table name extraction for shooting stats."""
        scraper = FBrefScraper()
        
        table_name = scraper._get_table_name("stats_shooting_12")
        
        assert table_name == "shooting"
    
    def test_get_filename_generation(self):
        """Test filename generation for table and season."""
        scraper = FBrefScraper()
        
        filename = scraper._get_filename("stats_standard_12", "2425")
        
        assert filename == "df_player_stats_2425.csv"
    
    @patch('pandas.read_html')
    def test_scrape_single_table_success(self, mock_read_html, setup_test_output_dir):
        """Test successful scraping of a single table."""
        # Mock the pandas.read_html to return a sample DataFrame
        mock_df = pd.DataFrame({
            'Player': ['Test Player'],
            'Position': ['Forward'],
            'Goals': [10]
        })
        mock_read_html.return_value = [mock_df]
        
        # Mock input to auto-confirm
        with patch('builtins.input', return_value='y'):
            scraper = FBrefScraper(output_dir=setup_test_output_dir)
            url = "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats"
            
            # Mock time.sleep to speed up test
            with patch('time.sleep'):
                result = scraper.scrape_squad_stats(url, force_overwrite=True)
        
        # Verify that read_html was called for all table IDs
        expected_calls = len(scraper.table_ids)
        assert mock_read_html.call_count == expected_calls
        
        # Verify that all expected table IDs were called
        called_table_ids = []
        for call in mock_read_html.call_args_list:
            attrs = call[1]['attrs']
            called_table_ids.append(attrs['id'])
        
        # Check that all expected table IDs were called
        for table_id in scraper.table_ids:
            assert table_id in called_table_ids
        
        # Verify result contains expected data
        assert len(result) == expected_calls
        assert "df_player_stats_2425" in result
        assert isinstance(result["df_player_stats_2425"], pd.DataFrame)
        
        # Verify files were saved to test output directory
        expected_files = [
            "df_player_stats_2425.csv",
            "df_player_shooting_2425.csv",
            "df_player_passing_2425.csv",
            "df_player_passing_types_2425.csv",
            "df_player_gca_2425.csv",
            "df_player_defense_2425.csv",
            "df_player_possession_2425.csv"
        ]
        
        for filename in expected_files:
            filepath = setup_test_output_dir / filename
            assert filepath.exists(), f"Expected file {filename} was not created"
    
    def test_calculate_estimated_time(self):
        """Test time estimation calculation."""
        scraper = FBrefScraper()
        
        estimated_time = scraper._calculate_estimated_time(7)
        
        # Should be a positive number
        assert estimated_time > 0
        # Should be reasonable (less than 2 hours for 7 tables)
        assert estimated_time < 7200
    
    def test_calculate_estimated_time_custom_delays(self):
        """Test time estimation with custom delay settings."""
        scraper = FBrefScraper(delay_range=(1, 2), cooldown_seconds=60)
        
        estimated_time = scraper._calculate_estimated_time(7)
        
        # Should be a positive number
        assert estimated_time > 0
        # Should be much shorter with custom settings
        assert estimated_time < 300  # Less than 5 minutes
    
    def test_format_time_seconds(self):
        """Test time formatting for seconds."""
        scraper = FBrefScraper()
        
        formatted = scraper._format_time(30)
        
        assert "seconds" in formatted
    
    def test_format_time_minutes(self):
        """Test time formatting for minutes."""
        scraper = FBrefScraper()
        
        formatted = scraper._format_time(120)
        
        assert "minutes" in formatted


class TestFBrefScraperIntegration:
    """Integration tests for FBref scraper with real data."""
    
    @pytest.fixture(autouse=True)
    def setup_integration_output_dir(self):
        """Create integration test output directory."""
        integration_output_dir = Path("tests", "tmp", "integration_output")
        integration_output_dir.mkdir(parents=True, exist_ok=True)
        yield integration_output_dir
    
    @pytest.mark.integration
    def test_scrape_real_valencia_data(self, setup_integration_output_dir):
        """Integration test: Scrape real Valencia CF data from FBref."""
        # Create scraper with faster settings for testing
        scraper = FBrefScraper(
            output_dir=setup_integration_output_dir,
            max_requests=10,
            cooldown_seconds=15 * 60,
            delay_range=(2, 4),  # Faster delays for testing
            current_season="2425"
        )
        
        url = "https://fbref.com/en/squads/dcc91a7b/Valencia-Stats"
        
        # Mock user input to auto-confirm
        with patch('builtins.input', return_value='y'):
            result = scraper.scrape_squad_stats(url, force_overwrite=True)
        
        # Verify we got real data
        assert len(result) > 0, "No data was scraped"
        
        # Check that we got all expected tables
        expected_tables = [
            "df_player_stats_2425",
            "df_player_shooting_2425", 
            "df_player_passing_2425",
            "df_player_passing_types_2425",
            "df_player_gca_2425",
            "df_player_defense_2425",
            "df_player_possession_2425"
        ]
        
        for table_name in expected_tables:
            assert table_name in result, f"Missing table: {table_name}"
            df = result[table_name]
            assert isinstance(df, pd.DataFrame), f"Table {table_name} is not a DataFrame"
            assert len(df) > 0, f"Table {table_name} is empty"
        
        # Verify files were created
        for table_name in expected_tables:
            filename = f"{table_name}.csv"
            filepath = setup_integration_output_dir / filename
            assert filepath.exists(), f"Expected file {filename} was not created"
            
            # Check file has content
            file_size = filepath.stat().st_size
            assert file_size > 100, f"File {filename} is too small ({file_size} bytes)"
        
        # Verify data structure (should have real Valencia players)
        stats_df = result["df_player_stats_2425"]
        assert "Player" in stats_df.columns, "Player column missing from stats table"
        assert len(stats_df) >= 20, f"Expected at least 20 players, got {len(stats_df)}"
        
        # Check for some expected Valencia players (these should exist)
        player_names = stats_df["Player"].astype(str).tolist()
        # Look for common Valencia players (case insensitive)
        valencia_players_found = any(
            any(name.lower() in player.lower() for name in ["Mamardashvili", "Guerra", "Duro", "López"])
            for player in player_names
        )
        assert valencia_players_found, "No expected Valencia players found in scraped data"
        
        print(f"\nIntegration test passed!")
        print(f"   Scraped {len(result)} tables")
        print(f"   Total players: {len(stats_df)}")
        print(f"   Output directory: {setup_integration_output_dir}")
        print(f"   Sample players: {player_names[:5]}")
    
    @pytest.mark.integration
    def test_scrape_real_liverpool_data(self, setup_integration_output_dir):
        """Integration test: Scrape real Liverpool data from FBref."""
        # Create scraper with faster settings for testing
        scraper = FBrefScraper(
            output_dir=setup_integration_output_dir,
            max_requests=10,
            cooldown_seconds=15 * 60,
            delay_range=(2, 4),  # Faster delays for testing
            current_season="2425"
        )
        
        url = "https://fbref.com/en/squads/822bd0ba/Liverpool-Stats"
        
        # Mock user input to auto-confirm
        with patch('builtins.input', return_value='y'):
            result = scraper.scrape_squad_stats(url, force_overwrite=True)
        
        # Verify we got real data
        assert len(result) > 0, "No data was scraped"
        
        # Check that we got all expected tables
        expected_tables = [
            "df_player_stats_2425",
            "df_player_shooting_2425", 
            "df_player_passing_2425",
            "df_player_passing_types_2425",
            "df_player_gca_2425",
            "df_player_defense_2425",
            "df_player_possession_2425"
        ]
        
        for table_name in expected_tables:
            assert table_name in result, f"Missing table: {table_name}"
            df = result[table_name]
            assert isinstance(df, pd.DataFrame), f"Table {table_name} is not a DataFrame"
            assert len(df) > 0, f"Table {table_name} is empty"
        
        # Verify data structure (should have real Liverpool players)
        stats_df = result["df_player_stats_2425"]
        assert "Player" in stats_df.columns, "Player column missing from stats table"
        assert len(stats_df) >= 20, f"Expected at least 20 players, got {len(stats_df)}"
        
        # Check for some expected Liverpool players
        player_names = stats_df["Player"].astype(str).tolist()
        # Look for common Liverpool players (case insensitive)
        liverpool_players_found = any(
            any(name.lower() in player.lower() for name in ["Salah", "Van Dijk", "Alisson", "Alexander-Arnold"])
            for player in player_names
        )
        assert liverpool_players_found, "No expected Liverpool players found in scraped data"
        
        print(f"\nLiverpool integration test passed!")
        print(f"   Scraped {len(result)} tables")
        print(f"   Total players: {len(stats_df)}")
        print(f"   Output directory: {setup_integration_output_dir}")
        print(f"   Sample players: {player_names[:5]}") 