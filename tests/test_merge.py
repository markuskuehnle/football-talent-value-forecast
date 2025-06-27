import sys
from pathlib import Path

from src.preprocessing.merge_player_data import run_merge_pipeline

def test_merge_pipeline():
    """Test the merge pipeline with Valencia CF data."""
    
    team_name = "Valencia CF"
    seasons = ["2223", "2324", "2425"]
    
    print(f"Testing merge pipeline for {team_name} with seasons: {seasons}")
    
    try:
        # Run the merge pipeline
        df_merged = run_merge_pipeline(team_name, seasons)
        
        print(f"\nMerge completed successfully!")
        print(f"Total rows: {len(df_merged)}")
        print(f"Columns: {list(df_merged.columns)}")
        
        # Check for Transfermarkt data
        market_value_count = df_merged['MarketValueEuro'].notna().sum()
        print(f"Rows with market value data: {market_value_count}")
        print(f"Percentage with market value: {market_value_count/len(df_merged)*100:.1f}%")
        
        # Show some sample data
        print("\nSample merged data:")
        sample_cols = ['Player', 'Season', 'MarketValueEuro', 'Age', 'Position']
        available_cols = [col for col in sample_cols if col in df_merged.columns]
        print(df_merged[available_cols].head(10))
        
        # Check specific player (Javi Guerra)
        javi_guerra_data = df_merged[df_merged['Player'].str.contains('Guerra', case=False, na=False)]
        if not javi_guerra_data.empty:
            print(f"\nJavi Guerra data found: {len(javi_guerra_data)} rows")
            print(javi_guerra_data[available_cols])
        else:
            print("\nNo Javi Guerra data found")
        
        return df_merged
        
    except Exception as e:
        print(f"Error during merge: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_merge_pipeline() 