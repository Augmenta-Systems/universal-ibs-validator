import pandas as pd
from typing import List, Optional

def apply_dominance_rule(
    df: pd.DataFrame, 
    value_col: str, 
    group_cols: List[str], 
    contributor_col: str,
    threshold: float = 0.60
) -> pd.DataFrame:
    """
    Implements the 'Dominance Rule' for Confidentiality.
    If a single contributor (e.g., a specific bank) provides > 60% of the total value
    for a specific aggregation (group_cols), the row is marked 'N' (Not Free).
    """
    # 1. Calculate Total per Group (e.g., Country + Sector)
    group_totals = df.groupby(group_cols)[value_col].transform('sum')
    
    # 2. Calculate Market Share of this specific row
    # (Assuming the input df is at the Bank level)
    df['market_share'] = df[value_col] / group_totals
    
    # 3. Determine Confidentiality Status
    # If share > 60%, it is identifying ('N'). Otherwise 'F'.
    df['CONFIDENTIALITY_STATUS'] = df['market_share'].apply(
        lambda share: 'N' if share > threshold else 'F'
    )
    
    return df.drop(columns=['market_share'])
