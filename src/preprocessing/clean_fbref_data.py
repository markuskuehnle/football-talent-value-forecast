# src/preprocessing/clean_fbref.py

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple


def load_clean_fbref_csv(path: str) -> pd.DataFrame:
    """Loads a FBref CSV where the first row is junk and the second row contains actual column names."""
    df = pd.read_csv(path, header=1)
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]
    return df


def load_fbref_season_data(season_code: str, base_path: Path) -> Dict[str, pd.DataFrame]:
    """Loads all FBref tables for a given season into a dictionary."""
    file_prefixes = [
        "player_stats",
        "player_shooting",
        "player_passing",
        "player_passing_types",
        "player_gca",
        "player_defense",
        "player_possession"
    ]

    dataframes = {}
    for prefix in file_prefixes:
        file_path = base_path / f"df_{prefix}_{season_code}.csv"
        if file_path.exists():
            df = load_clean_fbref_csv(file_path)
            dataframes[f"df_{prefix}_{season_code}"] = df
        else:
            print(f"[!] Missing: {file_path}")
    return dataframes


def drop_matches_column(fbref_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Drops the noisy 'Matches' column from all FBref tables if it exists."""
    for name, df in fbref_dfs.items():
        if "Matches" in df.columns:
            if df["Matches"].nunique() == 1 and df["Matches"].iloc[0] == "Matches":
                fbref_dfs[name] = df.drop(columns=["Matches"])
            else:
                print(f"[!] Warning: Unexpected values in 'Matches' column of {name}")
    return fbref_dfs


def find_players_in_multiple_seasons(
    season_dfs: List[Tuple[pd.DataFrame, str]]
) -> pd.DataFrame:
    """Identifies players appearing in multiple seasons."""
    all_players = []
    for df, season in season_dfs:
        if 'Player' not in df.columns:
            raise ValueError(f"Missing 'Player' column for season {season}")
        temp = df[["Player"]].copy()
        temp["Season"] = season
        all_players.append(temp)

    combined = pd.concat(all_players)
    grouped = combined.groupby("Player")["Season"].agg(lambda s: sorted(s.unique())).reset_index()
    return grouped[grouped["Season"].apply(len) > 1].rename(columns={"Season": "Seasons"})


def add_age_from_latest_season(players_df: pd.DataFrame, latest_df: pd.DataFrame) -> pd.DataFrame:
    """Maps player ages from the most recent season table."""
    age_map = latest_df[["Player", "Age"]].set_index("Player")["Age"]
    players_df["Age"] = players_df["Player"].map(age_map)
    return players_df


def save_to_interim(dataframes: Dict[str, pd.DataFrame], team_name: str) -> None:
    """Saves all cleaned dataframes to interim folder."""
    interim_dir = Path("..", "data", "interim", team_name, "fbref")
    interim_dir.mkdir(parents=True, exist_ok=True)

    for name, df in dataframes.items():
        out_path = interim_dir / f"{name}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved: {out_path}")
