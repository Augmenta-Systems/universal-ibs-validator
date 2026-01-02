import pandas as pd
import pytest
from universal_ibs_validator.confidentiality import apply_dominance_rule

def test_dominance_rule_flags_canada_as_confidential():
    """
    Scenario: 
    - Canada has two banks: Bank A (90) and Bank B (10). Total = 100.
    - Bank A holds 90% share.
    - Expectation: Bank A's rows (and the Canada aggregate) should be flagged 'N'.
    """
    # 1. Setup Data
    data = {
        'REP_CTY': ['CA', 'CA'],
        'CP_SECTOR': ['B', 'B'],     # Banking Sector
        'BANK_ID': ['BANK_A', 'BANK_B'],
        'VALUE': [90.0, 10.0]        # 90 + 10 = 100 Total
    }
    df = pd.DataFrame(data)

    # 2. Apply Logic
    # We group by Country + Sector to find the total market.
    # We check if any BANK_ID dominates that market.
    result_df = apply_dominance_rule(
        df, 
        value_col="VALUE", 
        group_cols=["REP_CTY", "CP_SECTOR"], 
        contributor_col="BANK_ID",
        threshold=0.60
    )

    # 3. Verify
    # Bank A (90/100 = 0.9) -> Should be 'N'
    bank_a_status = result_df.loc[result_df['BANK_ID'] == 'BANK_A', 'CONFIDENTIALITY_STATUS'].values[0]
    assert bank_a_status == 'N', f"Bank A should be Confidential (N), but got {bank_a_status}"

    # Bank B (10/100 = 0.1) -> Should be 'F' (Free) 
    # NOTE: In some regimes, if one is hidden, the other must be hidden to prevent calculation (residual disclosure).
    # For this specific unit test, we just check the dominance flag itself.
    bank_b_status = result_df.loc[result_df['BANK_ID'] == 'BANK_B', 'CONFIDENTIALITY_STATUS'].values[0]
    assert bank_b_status == 'F', f"Bank B should be Free (F), but got {bank_b_status}"

    print("\n✅ Unit Test Passed: Canada Dominance Logic is correct.")

if __name__ == "__main__":
    test_dominance_rule_flags_canada_as_confidential()
