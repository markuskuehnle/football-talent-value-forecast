import pandas as pd
from pathlib import Path
import ast
from typing import List, Dict, Optional, Tuple
from fuzzywuzzy import fuzz


def load_fbref_tables(data_dir: Path, prefixes: List[str], seasons: List[str]) -> Dict[str, pd.DataFrame]:
    combined_dataframes = {}

    for prefix in prefixes:
        dfs = []
        for season in seasons:
            file_path = data_dir / f"df_{prefix}_{season}.csv"
            if not file_path.exists():
                print(f"[WARN] Missing file: {file_path}")
                continue
            df = pd.read_csv(file_path)
            df["Season"] = season
            dfs.append(df)

        if not dfs:
            raise ValueError(f"No data found for prefix '{prefix}' in {data_dir}")
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_dataframes[prefix] = combined_df

    return combined_dataframes


def normalize_player_names(df: pd.DataFrame, col: str = "Player") -> pd.DataFrame:
    df = df.copy()
    df["NormalizedName"] = df[col].astype(str).str.lower().str.strip()
    return df


def merge_all_fbref_tables(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Inner-joins all FBref tables on NormalizedName and Season."""
    if "player_stats" not in dfs:
        raise ValueError("player_stats is required as base table")

    base = normalize_player_names(dfs["player_stats"])
    for name, df in dfs.items():
        if name == "player_stats":
            continue
        df = normalize_player_names(df)
        base = base.merge(
            df,
            on=["NormalizedName", "Season"],
            how="left",
            suffixes=("", f"_{name}")
        )
    return base


def parse_market_value(val: str) -> Optional[float]:
    if not isinstance(val, str):
        return None
    val = val.replace('€', '').replace(',', '').strip().lower()
    try:
        if 'm' in val:
            return float(val.replace('m', '')) * 1_000_000
        elif 'k' in val:
            return float(val.replace('k', '')) * 1_000
        return float(val)
    except ValueError:
        return None


def convert_season_format(season: str) -> str:
    """Convert between FBref format (2324) and Transfermarkt format (2023)."""
    if not isinstance(season, str):
        season = str(season)
    
    if len(season) == 4 and season.isdigit():
        # Transfermarkt format (2023) to FBref format (2324)
        if season.startswith('20'):
            year = int(season[2:4])
            return f"{year:02d}{(year + 1):02d}"
        else:
            # Already in FBref format
            return season
    elif len(season) == 4 and season.isdigit():
        # FBref format (2324) to Transfermarkt format (2023)
        year = int(season[:2])
        return f"20{year:02d}"
    
    return season


def load_clean_transfermarkt(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    
    # Check if we need to unpack Player column or if Name/Position already exist
    if 'Player' in df.columns and 'Name' not in df.columns:
        # Unpack 'Player' column if it exists
        df[['Name', 'Position']] = df['Player'].apply(lambda x: pd.Series(ast.literal_eval(x)))
        df.rename(columns={'Nat.': 'Nationality'}, inplace=True)
    
    # Normalize market value
    if 'Market value' in df.columns:
        df['MarketValueEuro'] = df['Market value'].apply(parse_market_value)
    elif 'MarketValueEuro' not in df.columns:
        raise ValueError("No market value column found in Transfermarkt data")
    
    # Convert season format from Transfermarkt (2023) to FBref (2324)
    df['Season'] = df['Season'].astype(str)
    df['Season'] = df['Season'].apply(convert_season_format)
    
    # Normalize player names
    df['NormalizedName'] = df['Name'].astype(str).str.lower().str.strip()

    return df[[
        "NormalizedName", "Season", "MarketValueEuro",
        "Age", "Position", "Nationality", "Current club"
    ]]


def merge_fbref_transfermarkt(fbref_df: pd.DataFrame, tm_df: pd.DataFrame) -> pd.DataFrame:
    """Merge FBref and Transfermarkt data with improved name matching."""
    
    # First try exact match on normalized names and season
    df_merged = fbref_df.merge(tm_df, on=["NormalizedName", "Season"], how="left")
    
    # For unmatched rows, try fuzzy matching
    unmatched_fbref = df_merged[df_merged['MarketValueEuro'].isna()].copy()
    matched_fbref = df_merged[df_merged['MarketValueEuro'].notna()].copy()
    
    if not unmatched_fbref.empty:
        print(f"Attempting fuzzy matching for {len(unmatched_fbref)} unmatched players...")
        
        # Get unique unmatched players
        unmatched_players = unmatched_fbref[['NormalizedName', 'Season']].drop_duplicates()
        
        for _, row in unmatched_players.iterrows():
            fbref_name = row['NormalizedName']
            season = row['Season']
            
            # Find best match in Transfermarkt for this season
            tm_season_data = tm_df[tm_df['Season'] == season]
            
            best_match = None
            best_score = 0
            
            for _, tm_row in tm_season_data.iterrows():
                tm_name = tm_row['NormalizedName']
                score = fuzz.token_sort_ratio(fbref_name, tm_name)
                
                if score > best_score and score >= 85:  # Threshold for good match
                    best_score = score
                    best_match = tm_row
            
            if best_match is not None:
                print(f"Fuzzy match: '{fbref_name}' -> '{best_match['NormalizedName']}' (score: {best_score})")
                
                # Update the unmatched rows with the matched data
                mask = (unmatched_fbref['NormalizedName'] == fbref_name) & (unmatched_fbref['Season'] == season)
                for col in ['MarketValueEuro', 'Age', 'Position', 'Nationality', 'Current club']:
                    unmatched_fbref.loc[mask, col] = best_match[col]
        
        # Recombine matched and unmatched data
        df_merged = pd.concat([matched_fbref, unmatched_fbref], ignore_index=True)

    # Clean up the merged data
    df_merged = clean_merged_data(df_merged)
    
    return df_merged


def remove_duplicate_columns(df_merged: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate columns, keeping only the original columns without suffixes."""
    
    # Find all columns that have the _player_* pattern (these are duplicates from joins)
    duplicate_columns = []
    original_columns = []
    
    for col in df_merged.columns:
        if '_player_' in col:
            # This is a duplicate column from a join
            duplicate_columns.append(col)
            # Extract the original column name (before the _player_ suffix)
            original_name = col.split('_player_')[0]
            if original_name not in original_columns:
                original_columns.append(original_name)
        else:
            # This is an original column
            original_columns.append(col)
    
    # Remove all duplicate columns
    if duplicate_columns:
        df_merged = df_merged.drop(columns=duplicate_columns)
        print(f"Removed {len(duplicate_columns)} duplicate columns with _player_* pattern")
        print(f"Removed columns: {duplicate_columns}")
    
    return df_merged


def clean_merged_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    """Clean the merged data by dropping unwanted columns and converting data types."""
    
    # Drop all Nation_* columns but keep the original Nation column
    nation_columns_to_drop = [col for col in df_merged.columns if col.startswith('Nation_')]
    if nation_columns_to_drop:
        df_merged = df_merged.drop(columns=nation_columns_to_drop)
        print(f"Dropped Nation_* columns: {nation_columns_to_drop}")
    
    # Drop all Age columns except Age_x, then rename Age_x to Age
    age_columns = [col for col in df_merged.columns if col.startswith('Age') and col != 'Age_x']
    if age_columns:
        df_merged = df_merged.drop(columns=age_columns)
        print(f"Dropped Age columns: {age_columns}")
    
    # Rename Age_x to Age if it exists
    if 'Age_x' in df_merged.columns:
        df_merged = df_merged.rename(columns={'Age_x': 'Age'})
        print(f"Renamed 'Age_x' to 'Age'")
    
    # Convert Age to integer, handling NaN values
    if 'Age' in df_merged.columns:
        # First try to convert to float, then to int
        df_merged['Age'] = pd.to_numeric(df_merged['Age'], errors='coerce')
        # Fill NaN with a default value (e.g., -1) or keep as NaN
        df_merged['Age'] = df_merged['Age'].fillna(-1).astype(int)
        print(f"Converted 'Age' to integer (NaN values set to -1)")
    
    # Remove duplicate columns
    df_merged = remove_duplicate_columns(df_merged)
    
    # Reorder columns in specific order: Player, Season, 'Nation', 'Nationality', 'Pos', 'Position', then all others
    if 'Player' in df_merged.columns and 'Season' in df_merged.columns:
        # Define the priority order for columns
        priority_columns = ['Player', 'Season', 'Nation', 'Nationality', 'Pos', 'Position']
        
        # Get columns that exist in the dataframe in priority order
        ordered_columns = []
        for col in priority_columns:
            if col in df_merged.columns:
                ordered_columns.append(col)
        
        # Add all remaining columns that weren't in the priority list
        remaining_columns = [col for col in df_merged.columns if col not in priority_columns]
        ordered_columns.extend(remaining_columns)
        
        # Reorder the dataframe
        df_merged = df_merged[ordered_columns]
        print(f"Reordered columns: {ordered_columns[:6]}... (and {len(remaining_columns)} others)")
    
    # Sort by player name and then by season chronologically
    if 'Player' in df_merged.columns and 'Season' in df_merged.columns:
        df_merged = df_merged.sort_values(['Player', 'Season'], ascending=[True, True])
        print(f"Sorted dataframe by Player name and Season chronologically")
    
    # Drop NormalizedName column LAST (after all matching and processing is complete)
    if 'NormalizedName' in df_merged.columns:
        df_merged = df_merged.drop(columns=['NormalizedName'])
        print(f"Dropped 'NormalizedName' column")
    
    return df_merged


def analyze_missing_market_values(df_merged: pd.DataFrame) -> Dict[str, any]:
    """Analyze why some players are missing market value data."""
    
    missing_market_values = df_merged[df_merged['MarketValueEuro'].isna()]
    
    # Handle NaN values in Player column by filling with a default value
    player_column_filled = missing_market_values['Player'].fillna('Unknown')
    
    # 1. System/Total rows
    system_rows = missing_market_values[player_column_filled.str.contains('Total|Squad|Opponent', case=False)]
    
    # 2. Incomplete names
    incomplete_names = missing_market_values[player_column_filled.str.len() < 5]
    
    # 3. New players in latest season (likely don't have market values yet)
    latest_season = df_merged['Season'].max()
    new_players_latest = missing_market_values[
        (missing_market_values['Season'] == latest_season) & 
        (~player_column_filled.str.contains('Total|Squad|Opponent', case=False))
    ]
    
    # 4. Players with name matching issues
    name_matching_issues = missing_market_values[
        (~player_column_filled.str.contains('Total|Squad|Opponent', case=False)) &
        (player_column_filled.str.len() >= 5) &
        (missing_market_values['Season'] != latest_season)
    ]
    
    analysis = {
        'total_missing': len(missing_market_values),
        'system_rows': len(system_rows),
        'incomplete_names': len(incomplete_names),
        'new_players_latest_season': len(new_players_latest),
        'name_matching_issues': len(name_matching_issues),
        'system_players': system_rows['Player'].unique().tolist(),
        'new_players': new_players_latest['Player'].unique().tolist(),
        'name_issues': name_matching_issues['Player'].unique().tolist()
    }
    
    return analysis


def filter_merged_data(df_merged: pd.DataFrame, remove_system_rows: bool = True, 
                      only_with_market_values: bool = False) -> pd.DataFrame:
    """Filter the merged data based on specified criteria."""
    
    df_filtered = df_merged.copy()
    
    if remove_system_rows:
        # Handle NaN values in Player column by filling with a default value
        player_column_filled = df_filtered['Player'].fillna('Unknown')
        # Remove system/total rows
        df_filtered = df_filtered[~player_column_filled.str.contains('Total|Squad|Opponent', case=False)]
        print(f"Removed system rows: {len(df_merged) - len(df_filtered)} rows")
    
    if only_with_market_values:
        # Only keep rows with market values
        df_filtered = df_filtered[df_filtered['MarketValueEuro'].notna()]
        print(f"Kept only rows with market values: {len(df_filtered)} rows")
    
    return df_filtered


def run_merge_pipeline(team_name: str, seasons: List[str], 
                      remove_system_rows: bool = True,
                      only_with_market_values: bool = False,
                      analyze_missing: bool = True) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Run the complete merge pipeline with optional filtering and analysis.
    
    Args:
        team_name: Name of the team
        seasons: List of seasons to merge
        remove_system_rows: Whether to remove system/total rows
        only_with_market_values: Whether to keep only rows with market values
        analyze_missing: Whether to analyze missing market values
    
    Returns:
        Tuple of (merged_dataframe, analysis_results)
    """
    
    # Try different possible data directory paths based on current working directory
    import os
    current_dir = Path.cwd()
    print(f"Current working directory: {current_dir}")
    
    # First, try to find the project root by looking for common project files
    project_root = None
    search_dir = current_dir
    for _ in range(5):  # Search up to 5 levels up
        if (search_dir / "pyproject.toml").exists() or (search_dir / "README.md").exists():
            project_root = search_dir
            break
        search_dir = search_dir.parent
    
    possible_paths = []
    
    # Prioritize project root data directory
    if project_root:
        project_data_dir = project_root / "data"
        possible_paths.append(project_data_dir)
        print(f"Found project root: {project_root}")
    
    # Then try other relative paths
    possible_paths.extend([
        Path("../data"),  # From notebooks directory
        Path("../../data"),  # From other subdirectories
        Path(current_dir.parent / "data"),  # From any subdirectory
        Path(current_dir.parent.parent / "data"),  # From deeper subdirectories
    ])
    
    # Only try current directory data as last resort (to avoid notebooks/data)
    possible_paths.append(Path("data"))
    
    print(f"Trying possible paths: {[str(p) for p in possible_paths]}")
    
    base_dir = None
    for path in possible_paths:
        print(f"Checking path: {path} - exists: {path.exists()}")
        if path.exists():
            # Verify this is the right data directory by checking for interim subdirectory
            if (path / "interim").exists():
                base_dir = path
                print(f"Found data directory: {base_dir}")
                break
            else:
                print(f"Path {path} exists but doesn't contain 'interim' subdirectory")
    
    if base_dir is None:
        raise FileNotFoundError(f"Data directory not found. Tried: {[str(p) for p in possible_paths]}")
    
    fbref_dir = base_dir / "interim" / team_name / "fbref"
    tm_path = base_dir / "interim" / team_name / "transfermarkt" / f"{team_name.lower().replace(' ', '_')}_2020_2024.csv"

    print(f"Looking for FBref directory: {fbref_dir}")
    print(f"Looking for Transfermarkt file: {tm_path}")

    # Check if directories exist
    if not fbref_dir.exists():
        raise FileNotFoundError(f"FBref directory not found: {fbref_dir}")
    if not tm_path.exists():
        raise FileNotFoundError(f"Transfermarkt file not found: {tm_path}")

    prefixes = [
        "player_stats", "player_shooting", "player_passing", "player_passing_types",
        "player_gca", "player_defense", "player_possession"
    ]

    fbref_tables = load_fbref_tables(fbref_dir, prefixes, seasons)
    df_fbref_all = merge_all_fbref_tables(fbref_tables)
    df_tm_clean = load_clean_transfermarkt(tm_path)

    df_merged = merge_fbref_transfermarkt(df_fbref_all, df_tm_clean)
    
    # Analyze missing market values if requested
    analysis = None
    if analyze_missing:
        analysis = analyze_missing_market_values(df_merged)
        
        # Print analysis summary
        print(f"\n=== MERGE ANALYSIS ===")
        print(f"Total rows: {len(df_merged)}")
        print(f"Rows with market value: {df_merged['MarketValueEuro'].notna().sum()}")
        print(f"Missing market values: {analysis['total_missing']}")
        print(f"  - System rows: {analysis['system_rows']}")
        print(f"  - Incomplete names: {analysis['incomplete_names']}")
        print(f"  - New players in {seasons[-1]}: {analysis['new_players_latest_season']}")
        print(f"  - Name matching issues: {analysis['name_matching_issues']}")
    
    # Filter data if requested
    if remove_system_rows or only_with_market_values:
        df_merged = filter_merged_data(df_merged, remove_system_rows, only_with_market_values)
    
    return df_merged, analysis
