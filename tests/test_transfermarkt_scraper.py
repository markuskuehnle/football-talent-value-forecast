import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from src.scrapers.transfermarkt_scraper import TransfermarktScraper, scrape_transfermarkt_team


class TestTransfermarktScraper:
    """Test cases for TransfermarktScraper class."""
    
    def test_initialization(self):
        """Test scraper initialization with default parameters."""
        scraper = TransfermarktScraper()
        
        assert scraper.output_dir == Path("data", "raw")
        assert scraper.delay_range == (1.0, 3.0)
        assert "Mozilla" in scraper.headers["User-Agent"]
    
    def test_initialization_with_custom_params(self):
        """Test scraper initialization with custom parameters."""
        custom_output_dir = Path("custom", "output")
        custom_delay_range = (2.0, 5.0)
        custom_headers = {"User-Agent": "Custom Agent"}
        
        scraper = TransfermarktScraper(
            output_dir=custom_output_dir,
            delay_range=custom_delay_range,
            headers=custom_headers
        )
        
        assert scraper.output_dir == custom_output_dir
        assert scraper.delay_range == custom_delay_range
        assert scraper.headers == custom_headers
    
    def test_extract_age_from_cell_valid(self):
        """Test age extraction from valid age text."""
        scraper = TransfermarktScraper()
        
        test_cases = [
            ("(24)", 24),
            ("(18)", 18),
            ("(30)", 30)
        ]
        
        for age_text, expected in test_cases:
            result = scraper._extract_age_from_cell(age_text)
            assert result == expected
    
    def test_extract_age_from_cell_invalid(self):
        """Test age extraction from invalid age text."""
        scraper = TransfermarktScraper()
        
        test_cases = ["", "No age", "25", None]
        
        for age_text in test_cases:
            result = scraper._extract_age_from_cell(age_text)
            assert result is None
    
    @patch('src.scrapers.transfermarkt_scraper.requests.get')
    def test_get_transfermarkt_club_url_success(self, mock_get):
        """Test successful club URL retrieval."""
        # Mock successful response
        mock_response = Mock()
        mock_response.content = '''
        <html>
            <a href="/fc-valencia/startseite/verein/1049">Valencia CF</a>
        </html>
        '''
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        scraper = TransfermarktScraper()
        result = scraper.get_transfermarkt_club_url("Valencia CF", country='EN')
        
        expected_url = "https://www.transfermarkt.en/fc-valencia/startseite/verein/1049"
        assert result == expected_url
    
    @patch('src.scrapers.transfermarkt_scraper.requests.get')
    def test_get_transfermarkt_club_url_not_found(self, mock_get):
        """Test club URL retrieval when club is not found."""
        # Mock response with no club links
        mock_response = Mock()
        mock_response.content = '<html><body>No clubs found</body></html>'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        scraper = TransfermarktScraper()
        result = scraper.get_transfermarkt_club_url("NonExistentTeam", country='EN')
        
        assert result is None
    
    @patch('src.scrapers.transfermarkt_scraper.requests.get')
    def test_get_transfermarkt_club_url_error(self, mock_get):
        """Test club URL retrieval when request fails."""
        # Mock request exception
        mock_get.side_effect = Exception("Network error")
        
        scraper = TransfermarktScraper()
        result = scraper.get_transfermarkt_club_url("Valencia CF", country='EN')
        
        assert result is None
    
    @patch.object(TransfermarktScraper, 'get_transfermarkt_club_url')
    def test_get_team_squad_url_with_dynamic_search(self, mock_get_club_url):
        """Test squad URL generation with dynamic club search."""
        # Mock successful club URL retrieval
        mock_get_club_url.return_value = "https://www.transfermarkt.com/fc-valencia/startseite/verein/1049"
        
        scraper = TransfermarktScraper()
        result = scraper._get_team_squad_url("Valencia CF", 2024)
        
        expected_url = "https://www.transfermarkt.com/fc-valencia/kader/verein/1049/saison_id/2024/plus/1"
        assert result == expected_url
    
    @patch.object(TransfermarktScraper, 'get_transfermarkt_club_url')
    def test_get_team_squad_url_fallback(self, mock_get_club_url):
        """Test squad URL generation with fallback to hardcoded Valencia CF."""
        # Mock failed club URL retrieval
        mock_get_club_url.return_value = None
        
        scraper = TransfermarktScraper()
        result = scraper._get_team_squad_url("Valencia CF", 2024)
        
        expected_url = "https://www.transfermarkt.com/fc-valencia/kader/verein/1049/saison_id/2024/plus/1"
        assert result == expected_url
    
    def test_get_team_squad_url_other_teams(self):
        """Test squad URL generation for other teams."""
        scraper = TransfermarktScraper()
        result = scraper._get_team_squad_url("Liverpool", 2024)
        
        # Should use fallback with team slug
        expected_url = "https://www.transfermarkt.com/liverpool/kader/verein/1049/saison_id/2024/plus/1"
        assert result == expected_url


class TestTransfermarktScraperIntegration:
    """Integration tests for Transfermarkt scraper (marked to avoid running by default)."""
    
    @pytest.mark.integration
    @pytest.mark.skip(reason="Integration test - requires network access")
    def test_scrape_team_season_integration(self):
        """Integration test for scraping a single season."""
        scraper = TransfermarktScraper()
        result = scraper.scrape_team_season("Valencia CF", 2024)
        
        # Should return a DataFrame with player data
        assert not result.empty
        assert "Player" in result.columns
        assert "Market value" in result.columns
        assert "Season" in result.columns
        assert len(result) > 0
    
    @pytest.mark.integration
    @pytest.mark.skip(reason="Integration test - requires network access")
    def test_scrape_transfermarkt_team_integration(self):
        """Integration test for the convenience function."""
        result = scrape_transfermarkt_team("Valencia CF", 2024, 2024)
        
        # Should return a DataFrame with player data
        assert not result.empty
        assert "Player" in result.columns
        assert "Market value" in result.columns
        assert "Season" in result.columns
        assert len(result) > 0


def test_scrape_transfermarkt_team_convenience_function():
    """Test the convenience function creates scraper correctly."""
    with patch('src.scrapers.transfermarkt_scraper.TransfermarktScraper') as mock_scraper_class:
        mock_scraper = Mock()
        mock_scraper_class.return_value = mock_scraper
        mock_scraper.scrape_team.return_value = "mock_result"
        
        result = scrape_transfermarkt_team("Valencia CF", 2020, 2024)
        
        # Should create scraper and call scrape_team
        mock_scraper_class.assert_called_once()
        mock_scraper.scrape_team.assert_called_once_with(
            team_name="Valencia CF",
            min_season=2020,
            max_season=2024,
            output_filename=None,
            drop_metadata_columns=True
        )
        assert result == "mock_result" 