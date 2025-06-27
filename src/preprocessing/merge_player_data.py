import pandas as pd
from pathlib import Path
import ast
from typing import List, Dict, Optional
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
    if len(season) == 4 and season.isdigit():
        # Transfermarkt format (2023) to FBref format (2324)
        if season.startswith('20'):
            return f"{season[2:4]}{str(int(season[2:4]) + 1).zfill(2)}"
        else:
            # Already in FBref format
            return season
    else:
        # FBref format (2324) to Transfermarkt format (2023)
        if len(season) == 4 and season.isdigit():
            return f"20{season[:2]}"
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
    
    return df_merged


def run_merge_pipeline(team_name: str, seasons: List[str]) -> pd.DataFrame:
    base_dir = Path("data")
    fbref_dir = base_dir / "interim" / team_name / "fbref"
    tm_path = base_dir / "interim" / team_name / "transfermarkt" / f"{team_name.lower().replace(' ', '_')}_market_value_22_25.csv"

    prefixes = [
        "player_stats", "player_shooting", "player_passing", "player_passing_types",
        "player_gca", "player_defense", "player_possession"
    ]

    fbref_tables = load_fbref_tables(fbref_dir, prefixes, seasons)
    df_fbref_all = merge_all_fbref_tables(fbref_tables)
    df_tm_clean = load_clean_transfermarkt(tm_path)

    df_merged = merge_fbref_transfermarkt(df_fbref_all, df_tm_clean)

    return df_merged
