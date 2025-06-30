# src/preprocessing/parse_transfermarkt.py

import pandas as pd
import ast
from pathlib import Path


def parse_market_value(val: str) -> float | None:
    """Convert strings like '€1.5m' or '€900k' into float euros."""
    if isinstance(val, str):
        val = val.replace('€', '').replace(',', '').strip().lower()
        if 'm' in val:
            return float(val.replace('m', '')) * 1_000_000
        elif 'k' in val:
            return float(val.replace('k', '')) * 1_000
    return None


def clean_transfermarkt_dataframe(filepath: str) -> pd.DataFrame:
    """Loads and cleans a Transfermarkt market value CSV."""
    df = pd.read_csv(filepath)

    # Extract 'Name' and 'Position' from Player column (stored as stringified tuple/list)
    df[['Name', 'Position']] = df['Player'].apply(lambda x: pd.Series(ast.literal_eval(x)))
    df.rename(columns={'Nat.': 'Nationality'}, inplace=True)

    # Parse market value
    df['MarketValueEuro'] = df['Market value'].apply(parse_market_value)

    # Define columns to keep, checking if they exist
    columns_to_keep: list[str] = ['Name', 'Position', 'Age', 'Nationality', 'Current club', 'Season', 'MarketValueEuro']
    
    # Add '#' column if it exists, otherwise skip it
    if '#' in df.columns:
        columns_to_keep.insert(0, '#')
    
    # Reduce to useful subset
    df_clean = df[columns_to_keep].dropna(subset=['MarketValueEuro', 'Season'])
    
    # Rename '#' to 'Rank' if it exists
    if '#' in df_clean.columns:
        df_clean = df_clean.rename(columns={'#': 'Rank'})

    # Type cast + normalization
    df_clean['Season'] = df_clean['Season'].astype(str)

    df_clean['Nationality'] = df_clean['Nationality'].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x
    )
    df_clean['PrimaryNationality'] = df_clean['Nationality'].apply(
        lambda x: x[0] if isinstance(x, list) else x
    )

    return df_clean


def explode_nationalities(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Explodes the nationality column into multiple rows per player if multiple nationalities exist."""
    df = df_clean.copy()
    df['Nationality'] = df['Nationality'].apply(lambda x: x if isinstance(x, list) else [x])
    df_exploded = df.explode('Nationality').reset_index(drop=True)
    return df_exploded
