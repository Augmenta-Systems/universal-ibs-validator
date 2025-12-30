"""
LBS (Locational Banking Statistics) Rule Definitions.

This module consolidates logic from:
- check_lbs_r.py (Residency Internal Rules)
- check_lbs_n.py (Nationality Internal Rules)
- check_lbs.py (Cross-Report Consistency Rules)

It implements three categories of rules:
1. LBSR Internal: Validates the Residency report.
2. LBSN Internal: Validates the Nationality report.
3. Cross-Report: Validates consistency between Residency and Nationality.
"""

from typing import List
from ..domain_types import ValidationRule, ValidationContext

# -----------------------------------------------------------------------------
# SHARED CONSTANTS & DIMENSIONS
# -----------------------------------------------------------------------------

# Dimensions used for Internal Consistency Aggregations (LBS)
# We group by these to verify that components sum up to the total.
LBS_INTERNAL_JOIN_DIMS = [
    "POSITION", "REP_CTY"
]

# Dimensions used for Cross-Report Checks (LBSR vs LBSN)
# These define the granular "cell" that must match between the two reports.
LBS_CROSS_JOIN_DIMS = [
    "POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", 
    "REP_CTY", "CP_SECTOR", "CP_COUNTRY"
]

# -----------------------------------------------------------------------------
# 1. COMMON RULES (Shared by LBSR and LBSN)
# -----------------------------------------------------------------------------

def _get_common_internal_rules(prefix: str = "LBS") -> List[ValidationRule]:
    """
    Returns the fundamental aggregation rules that apply to BOTH LBSR and LBSN.
    (Currency, Sector, Instrument, Bank Type).
    """
    rules = []

    # --- Section A: Currency Breakdown ---
    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC01",
        description="Currency Aggregates: All currencies (TO1:A) = Domestic (Local:D) + Foreign (TO1:F)",
        lhs_filter=lambda df, ctx: df[
            (df.DENOM == "TO1") & (df.CURR_TYPE == "A")
        ],
        rhs_filter=lambda df, ctx: df[
            ((df.DENOM == ctx.currency_code) & (df.CURR_TYPE == "D")) | 
            ((df.DENOM == "TO1") & (df.CURR_TYPE == "F"))
        ],
        join_dims=["POSITION", "INSTRUMENT", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
        operator='eq',
        tolerance=4.0
    ))

    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC02",
        description="Foreign Currency Aggregate: Total Foreign (TO1:F) = Sum of Major Currencies + Other (TO3:F)",
        lhs_filter=lambda df, ctx: df[
            (df.DENOM == "TO1") & (df.CURR_TYPE == "F")
        ],
        rhs_filter=lambda df, ctx: df[
            (df.DENOM.isin(["USD", "EUR", "JPY", "CHF", "GBP", "TO3"])) & 
            (df.CURR_TYPE == "F")
        ],
        join_dims=["POSITION", "INSTRUMENT", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
        operator='eq',
        tolerance=4.0
    ))

    # --- Section B: Counterparty Sector Breakdown ---
    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC04",
        description="Sector Aggregate: All Sectors (A) = Banks (B) + Non-Banks (N) + Unallocated (U)",
        lhs_filter=lambda df, ctx: df[
            (df.INSTRUMENT != "M") & (df.CP_SECTOR == "A")
        ],
        rhs_filter=lambda df, ctx: df[
            (df.INSTRUMENT != "M") & (df.CP_SECTOR.isin(["B", "N", "U"]))
        ],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
        operator='eq',
        tolerance=9.0
    ))

    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC05",
        description="Bank Sector: Banks (B) = Related (I) + Central Bank (M) + Unrelated (J)",
        lhs_filter=lambda df, ctx: df[df.CP_SECTOR == "B"],
        rhs_filter=lambda df, ctx: df[df.CP_SECTOR.isin(["I", "M", "J"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
        operator='eq',
        tolerance=9.0
    ))

    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC06",
        description="Non-Bank Sector: Non-Banks (N) = Financial (F) + Non-Financial (P)",
        lhs_filter=lambda df, ctx: df[df.CP_SECTOR == "N"],
        rhs_filter=lambda df, ctx: df[df.CP_SECTOR.isin(["F", "P"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
        operator='eq',
        tolerance=9.0
    ))

    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC07",
        description="Non-Financial Sector: P = Corporate (C) + Government (G) + Households (H)",
        lhs_filter=lambda df, ctx: df[df.CP_SECTOR == "P"],
        rhs_filter=lambda df, ctx: df[df.CP_SECTOR.isin(["C", "G", "H"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
        operator='eq',
        tolerance=9.0
    ))

    # --- Section C: Instrument Breakdown ---
    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC08",
        description="Instrument Aggregate: All Instruments (A) = Debt (D) + Loans/Dep (G) + Other (I)",
        lhs_filter=lambda df, ctx: df[df.INSTRUMENT == "A"],
        rhs_filter=lambda df, ctx: df[df.INSTRUMENT.isin(["D", "G", "I"])], # Some banks use D,G,V,K here
        join_dims=["POSITION", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
        operator='eq',
        tolerance=3.0
    ))
    
    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC28",
        description="Other Instruments: Other (I) = Derivatives (V) + Other (K)",
        lhs_filter=lambda df, ctx: df[df.INSTRUMENT == "I"],
        rhs_filter=lambda df, ctx: df[df.INSTRUMENT.isin(["V", "K"])],
        join_dims=["POSITION", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
        operator='eq',
        tolerance=3.0
    ))

    # --- Section D: Bank Type Breakdown ---
    rules.append(ValidationRule(
        rule_id=f"{prefix}_CC10",
        description="Bank Type: All Banks (A) = Domestic (D) + Foreign Branches (B) + Foreign Subs (S)",
        lhs_filter=lambda df, ctx: df[df.REP_BANK_TYPE == "A"],
        rhs_filter=lambda df, ctx: df[df.REP_BANK_TYPE.isin(["D", "B", "S"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
        operator='eq',
        tolerance=7.0
    ))

    return rules


# -----------------------------------------------------------------------------
# 2. RESIDENCY INTERNAL RULES (LBSR)
# -----------------------------------------------------------------------------

def get_lbsr_internal_rules() -> List[ValidationRule]:
    """
    Returns rules for validating LBSR internal aggregation logic.
    Includes Common Rules + Residency Specific Rules (Section F).
    """
    # 1. Get Common Rules
    rules = _get_common_internal_rules(prefix="LBSR")

    # 2. Add Residency Specific Rules (Section F in BIS XLS)
    rules.append(ValidationRule(
        rule_id="LBSR_CC14",
        description="Residency: All Countries (5J) = Residents (Reporting Country) + Non-Residents (5Z) + Unallocated (5M)",
        lhs_filter=lambda df, ctx: df[df.CP_COUNTRY == "5J"],
        rhs_filter=lambda df, ctx: df[df.CP_COUNTRY.isin([ctx.reporting_country, "5Z", "5M"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR"],
        operator='eq',
        tolerance=10.0
    ))

    rules.append(ValidationRule(
        rule_id="LBSR_CC15",
        description="Non-Residents (5Z) = Sum of all Non-Resident Regions (excluding Home, 5M, 5J, 5Z)",
        lhs_filter=lambda df, ctx: df[df.CP_COUNTRY == "5Z"],
        rhs_filter=lambda df, ctx: df[~df.CP_COUNTRY.isin([ctx.reporting_country, "5M", "5J", "5Z"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR"],
        operator='eq',
        tolerance=10.0
    ))

    return rules


# -----------------------------------------------------------------------------
# 3. NATIONALITY INTERNAL RULES (LBSN)
# -----------------------------------------------------------------------------

def get_lbsn_internal_rules() -> List[ValidationRule]:
    """
    Returns rules for validating LBSN internal aggregation logic.
    Includes Common Rules + Parent Country Specific Rules (Section E).
    """
    # 1. Get Common Rules
    rules = _get_common_internal_rules(prefix="LBSN")

    # 2. Add Parent Country Specific Rules (Section E in BIS XLS)
    # LBSN aggregates by PARENT_CTY, whereas LBSR aggregates by CP_COUNTRY.
    
    rules.append(ValidationRule(
        rule_id="LBSN_CC11",
        description="Parent Country: All Countries (5J) = BIS Parents (5L) + Non-BIS Parents (5X) + Consortium (1G) + Unallocated (5M)",
        lhs_filter=lambda df, ctx: df[df.PARENT_CTY == "5J"],
        rhs_filter=lambda df, ctx: df[df.PARENT_CTY.isin(["5L", "5X", "1G", "5M"])],
        join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
        operator='eq',
        tolerance=7.0
    ))

    # Note: CC12 (BIS Parents breakdown) and CC13 (Non-BIS Parents breakdown) 
    # require lists of specific ISO codes (AU, AT, BE... vs 5N, 1Q...).
    # For a universal validator, we often check 5L vs (All - 5X - 1G - 5M) or strictly defined lists.
    # Below is a simplified logical check for 5L + 5X + 1G + 5M summing to Total (CC11 covers the high level).
    
    return rules


# -----------------------------------------------------------------------------
# 4. CROSS-REPORT RULES (LBSR vs LBSN)
# -----------------------------------------------------------------------------

def get_lbs_cross_report_rules() -> List[ValidationRule]:
    """
    Returns rules for validating consistency between LBSR and LBSN.
    See: check_lbs.py.
    
    LHS = LBSR Data
    RHS = LBSN Data
    """
    return [
        ValidationRule(
            rule_id="LBS_CC22",
            description="LBP_R: Claims (All Banks) == LBP_N: Claims (All Parents)",
            # LBP_R: Position C, Inst A, Parent 5J, BankType A
            lhs_filter=lambda df, ctx: df[
                (df.POSITION == "C") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "A")
            ],
            # LBP_N: Same filters
            rhs_filter=lambda df, ctx: df[
                (df.POSITION == "C") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "A")
            ],
            join_dims=LBS_CROSS_JOIN_DIMS,
            operator='eq',
            tolerance=4.0
        ),
        ValidationRule(
            rule_id="LBS_CC24",
            description="LBP_R: Claims (Dom Banks) == LBP_N: Claims (Parents in Rep Country)",
            # LBP_R: Position C, Inst A, Parent 5J, BankType D
            lhs_filter=lambda df, ctx: df[
                (df.POSITION == "C") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "D")
            ],
            # LBP_N: Position C, Inst A, Parent [RepCountry], BankType A
            rhs_filter=lambda df, ctx: df[
                (df.POSITION == "C") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == ctx.reporting_country) & 
                (df.REP_BANK_TYPE == "A")
            ],
            join_dims=LBS_CROSS_JOIN_DIMS,
            operator='eq',
            tolerance=6.0
        ),
        ValidationRule(
            rule_id="LBS_CC23",
            description="LBP_R: Liabilities (All Banks) == LBP_N: Liabilities (All Parents)",
            lhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "A")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "A")
            ],
            join_dims=LBS_CROSS_JOIN_DIMS,
            operator='eq',
            tolerance=4.0
        ),
        ValidationRule(
            rule_id="LBS_CC25",
            description="LBP_R: Liabilities (Dom Banks) == LBP_N: Liabilities (Parents in Rep Country)",
            lhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "D")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT == "A") & 
                (df.PARENT_CTY == ctx.reporting_country) & 
                (df.REP_BANK_TYPE == "A")
            ],
            join_dims=LBS_CROSS_JOIN_DIMS,
            operator='eq',
            tolerance=6.0
        ),
        ValidationRule(
            rule_id="LBS_CC26",
            description="LBP_R: Liab (Debt/M) (All Banks) == LBP_N: Liab (Debt/M) (All Parents)",
            lhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT.isin(["D", "M"])) & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "A")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT.isin(["D", "M"])) & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "A")
            ],
            join_dims=LBS_CROSS_JOIN_DIMS,
            operator='eq',
            tolerance=4.0
        ),
        ValidationRule(
            rule_id="LBS_CC27",
            description="LBP_R: Liab (Debt/M) (Dom Banks) == LBP_N: Liab (Debt/M) (Parents in Rep Country)",
            lhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT.isin(["D", "M"])) & 
                (df.PARENT_CTY == "5J") & 
                (df.REP_BANK_TYPE == "D")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.POSITION == "L") & 
                (df.INSTRUMENT.isin(["D", "M"])) & 
                (df.PARENT_CTY == ctx.reporting_country) & 
                (df.REP_BANK_TYPE == "A")
            ],
            join_dims=LBS_CROSS_JOIN_DIMS,
            operator='eq',
            tolerance=6.0
        ),
    ]
