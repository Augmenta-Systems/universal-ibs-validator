"""
LBS (Locational Banking Statistics) Rule Definitions.

This module contains two types of rules:
1. Internal Consistency (LBSR): Verifies aggregations within the Residency report.
2. Cross-Report Consistency (LBSR vs LBSN): Verifies that Residency and Nationality reports match.
"""

from typing import List
from ..domain_types import ValidationRule, ValidationContext

# Common Dimensions used for joining/merging in LBS
# We exclude dimensions that are specifically filtered in the rules (like CURR_TYPE or PARENT_CTY)
# to avoid merge conflicts, but keep the core dimensions that define the "slice".
LBS_INTERNAL_JOIN_DIMS = [
    "POSITION", "REP_CTY"
]

LBS_CROSS_JOIN_DIMS = [
    "POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", 
    "REP_CTY", "CP_SECTOR", "CP_COUNTRY"
]

def get_lbsr_internal_rules() -> List[ValidationRule]:
    """
    Returns rules for validating LBSR internal aggregation logic.
    For these rules, the Engine should be called with lhs_df=LBSR and rhs_df=LBSR.
    """
    return [
        # -------------------------------------------------------------------------
        # SECTION A: Currency Breakdown Checks
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id="LBS_CC01",
            description="Currency Aggregates: All currencies (TO1:A) = Domestic (Local:D) + Foreign (TO1:F)",
            lhs_filter=lambda df, ctx: df[
                (df.DENOM == "TO1") & (df.CURR_TYPE == "A")
            ],
            rhs_filter=lambda df, ctx: df[
                (
                    (df.DENOM == ctx.currency_code) & (df.CURR_TYPE == "D")
                ) | (
                    (df.DENOM == "TO1") & (df.CURR_TYPE == "F")
                )
            ],
            join_dims=["POSITION", "INSTRUMENT", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
            operator='eq',
            tolerance=4.0
        ),
        ValidationRule(
            rule_id="LBS_CC02",
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
        ),

        # -------------------------------------------------------------------------
        # SECTION B: Sector Breakdown Checks
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id="LBS_CC04",
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
        ),
        ValidationRule(
            rule_id="LBS_CC05",
            description="Bank Sector: Banks (B) = Related (I) + Central Bank (M) + Unrelated (J)",
            lhs_filter=lambda df, ctx: df[
                df.CP_SECTOR == "B"
            ],
            rhs_filter=lambda df, ctx: df[
                df.CP_SECTOR.isin(["I", "M", "J"])
            ],
            join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
            operator='eq',
            tolerance=9.0
        ),
        ValidationRule(
            rule_id="LBS_CC06",
            description="Non-Bank Sector: Non-Banks (N) = Financial (F) + Non-Financial (P)",
            lhs_filter=lambda df, ctx: df[
                df.CP_SECTOR == "N"
            ],
            rhs_filter=lambda df, ctx: df[
                df.CP_SECTOR.isin(["F", "P"])
            ],
            join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
            operator='eq',
            tolerance=9.0
        ),
        ValidationRule(
            rule_id="LBS_CC07",
            description="Non-Financial Sector: P = Corporate (C) + Government (G) + Households (H)",
            lhs_filter=lambda df, ctx: df[
                df.CP_SECTOR == "P"
            ],
            rhs_filter=lambda df, ctx: df[
                df.CP_SECTOR.isin(["C", "G", "H"])
            ],
            join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_COUNTRY"],
            operator='eq',
            tolerance=9.0
        ),

        # -------------------------------------------------------------------------
        # SECTION C: Instrument Breakdown Checks
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id="LBS_CC08",
            description="Instrument Aggregate: All Instruments (A) = Debt (D) + Loans/Dep (G) + Other (I) + Unallocated (U)",
            lhs_filter=lambda df, ctx: df[
                df.INSTRUMENT == "A"
            ],
            rhs_filter=lambda df, ctx: df[
                df.INSTRUMENT.isin(["D", "G", "I", "U"]) # Note: Added I based on logic, usually V+K are subcomponents of I
            ],
            # Note: CC08 in check_lbs_r.py compared A vs D+G+V+K. 
            # If 'I' (Derivatives+Other) is reported directly, use I. If breakdown V+K is reported, use V+K.
            # Based on check_lbs_r.py source: LHS=A, RHS=D,G,V,K. 
            join_dims=["POSITION", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
            operator='eq',
            tolerance=3.0
        ),
        ValidationRule(
            rule_id="LBS_CC28",
            description="Other Instruments: Other (I) = Derivatives (V) + Other (K)",
            lhs_filter=lambda df, ctx: df[
                df.INSTRUMENT == "I"
            ],
            rhs_filter=lambda df, ctx: df[
                df.INSTRUMENT.isin(["V", "K"])
            ],
            join_dims=["POSITION", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
            operator='eq',
            tolerance=3.0
        ),

        # -------------------------------------------------------------------------
        # SECTION D: Bank Type Breakdown Checks
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id="LBS_CC10",
            description="Bank Type: All Banks (A) = Domestic (D) + Foreign Branches (B) + Foreign Subs (S)",
            lhs_filter=lambda df, ctx: df[
                df.REP_BANK_TYPE == "A"
            ],
            rhs_filter=lambda df, ctx: df[
                df.REP_BANK_TYPE.isin(["D", "B", "S"])
            ],
            join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_CTY", "CP_SECTOR", "CP_COUNTRY"],
            operator='eq',
            tolerance=7.0
        ),

        # -------------------------------------------------------------------------
        # SECTION F: Counterparty Country Checks
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id="LBS_CC14",
            description="Residency: All Countries (5J) = Residents (Home) + Non-Residents (5Z) + Unallocated (5M)",
            lhs_filter=lambda df, ctx: df[
                df.CP_COUNTRY == "5J"
            ],
            rhs_filter=lambda df, ctx: df[
                df.CP_COUNTRY.isin([ctx.reporting_country, "5Z", "5M"])
            ],
            join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR"],
            operator='eq',
            tolerance=10.0
        ),
        ValidationRule(
            rule_id="LBS_CC15",
            description="Non-Residents: 5Z = Sum of Regions (e.g., 4W + 4Y + etc.) or all Non-Resident ISOs",
            # Note: check_lbs_r.py specifically checks 5Z against specific exclusions or inclusions.
            # Implementing the logic: 5Z vs All countries NOT in {CA, 5M, 5J, 5Z}
            lhs_filter=lambda df, ctx: df[
                df.CP_COUNTRY == "5Z"
            ],
            rhs_filter=lambda df, ctx: df[
                ~df.CP_COUNTRY.isin([ctx.reporting_country, "5M", "5J", "5Z"])
            ],
            join_dims=["POSITION", "INSTRUMENT", "DENOM", "CURR_TYPE", "PARENT_CTY", "REP_BANK_TYPE", "REP_CTY", "CP_SECTOR"],
            operator='eq',
            tolerance=10.0
        ),
    ]


def get_lbs_cross_report_rules() -> List[ValidationRule]:
    """
    Returns rules for validating consistency between LBSR and LBSN.
    For these rules, the Engine should be called with lhs_df=LBSR and rhs_df=LBSN.
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
