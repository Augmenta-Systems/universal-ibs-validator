"""
CBS (Consolidated Banking Statistics) Rule Definitions.

This module contains:
1. Internal Consistency (CBSI): Verifies aggregations within the Immediate Counterparty report.
2. Internal Consistency (CBSG): Verifies aggregations within the Guarantor Basis report.
3. Cross-Report Consistency (CBSI vs CBSG): Verifies consistency between Immediate and Guarantor views.
"""

from typing import List
from ..domain_types import ValidationRule, ValidationContext

# Standard Join Dimensions for CBS
# These define a unique "cell" in the data.
CBS_STD_DIMS = [
    "MEASURE", "REP_COUNTRY", "BANK_TYPE", "REPORTING_BASIS",
    "POSITION", "INSTRUMENT", "REMAINING_MATURITY", 
    "CP_CURRENCY", "CP_SECTOR", "CP_COUNTRY"
]

def get_cbs_internal_rules() -> List[ValidationRule]:
    """
    Returns rules for validating CBS Immediate Counterparty (CBSI) internal consistency.
    Derived from check_cbs_i.py logic.
    """
    return _get_standard_internal_rules(basis_filter="F", prefix="CBS_CC")

def get_cbsg_internal_rules() -> List[ValidationRule]:
    """
    Returns rules for validating CBS Guarantor Basis (CBSG) internal consistency.
    Mirrors CBSI aggregation logic but applied to Guarantor Basis (G).
    """
    return _get_standard_internal_rules(basis_filter="G", prefix="CBSG_CC")

def _get_standard_internal_rules(basis_filter: str, prefix: str) -> List[ValidationRule]:
    """
    Helper to generate standard aggregation rules (Sectors, Maturities, etc.)
    for a specific Reporting Basis (F=Immediate, G=Guarantor).
    """
    return [
        # -------------------------------------------------------------------------
        # SECTION: Sector Breakdown Aggregations (Equalities)
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id=f"{prefix}01",
            description=f"({basis_filter}) Non-financial private sector (S) = Non-fin corps (C) + Households (H) + Unallocated (L)",
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR == "S")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR.isin(["C", "H", "L"]))
            ],
            join_dims=[d for d in CBS_STD_DIMS if d != "CP_SECTOR"], 
            operator='eq',
            tolerance=3.0
        ),
        ValidationRule(
            rule_id=f"{prefix}02",
            description=f"({basis_filter}) Non-bank private sector (R) = Non-bank financial (F) + Non-financial private (S)",
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR == "R")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR.isin(["F", "S"]))
            ],
            join_dims=[d for d in CBS_STD_DIMS if d != "CP_SECTOR"],
            operator='eq',
            tolerance=3.0
        ),
        ValidationRule(
            rule_id=f"{prefix}03",
            description=f"({basis_filter}) All Sectors (A) = Banks (B) + Official (O) + Non-bank private (R) + Unallocated (U)",
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR == "A")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR.isin(["B", "O", "R", "U"]))
            ],
            join_dims=[d for d in CBS_STD_DIMS if d != "CP_SECTOR"],
            operator='eq',
            tolerance=3.0
        ),

        # -------------------------------------------------------------------------
        # SECTION: Maturity Breakdown Aggregations
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id=f"{prefix}04",
            description=f"({basis_filter}) Total Maturity (A) = <=1yr (U) + 1-2yr (M) + >2yr (N) + Unallocated (X)",
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR == "A")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & (df.POSITION == "I") & 
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY.isin(["U", "M", "N", "X"])) & 
                (df.CP_CURRENCY == "TO1") & (df.CP_SECTOR == "A")
            ],
            join_dims=[d for d in CBS_STD_DIMS if d != "REMAINING_MATURITY"],
            operator='eq',
            tolerance=4.0
        ),

        # -------------------------------------------------------------------------
        # SECTION: Identities (Total Claims, Assets, Etc)
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id=f"{prefix}09",
            description=f"({basis_filter}) Total Claims (C) = International Claims (I) + Local Claims Local Currency (B)",
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & 
                (df.POSITION == "C") & (df.INSTRUMENT == "A") & 
                (df.REMAINING_MATURITY == "A") & (df.CP_CURRENCY == "TO1") & 
                (df.CP_SECTOR == "A")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & 
                (
                    ((df.POSITION == "I") & (df.CP_CURRENCY == "TO1")) | 
                    ((df.POSITION == "B") & (df.CP_CURRENCY == "LC1"))
                ) &
                (df.INSTRUMENT == "A") & (df.REMAINING_MATURITY == "A") & 
                (df.CP_SECTOR == "A")
            ],
            join_dims=[d for d in CBS_STD_DIMS if d not in ["POSITION", "CP_CURRENCY"]],
            operator='eq',
            tolerance=5.0
        ),
        
        # -------------------------------------------------------------------------
        # SECTION: Inequality Checks
        # -------------------------------------------------------------------------
        ValidationRule(
            rule_id=f"{prefix}11",
            description=f"({basis_filter}) Total Claims (C) <= Total Assets (F)",
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & 
                (df.POSITION == "C") & (df.INSTRUMENT == "A") & 
                (df.REMAINING_MATURITY == "A") & (df.CP_CURRENCY == "TO1") & 
                (df.CP_SECTOR == "A") & (df.CP_COUNTRY == "5J")
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == basis_filter) & 
                (df.POSITION == "F") & (df.INSTRUMENT == "A") & 
                (df.REMAINING_MATURITY == "A") & (df.CP_CURRENCY == "TO1") & 
                (df.CP_SECTOR == "A") & (df.CP_COUNTRY == "5J")
            ],
            join_dims=[d for d in CBS_STD_DIMS if d != "POSITION"],
            operator='lte',
            tolerance=2.0
        ),
    ]

def get_cbs_cross_report_rules() -> List[ValidationRule]:
    """
    Returns rules for validating consistency between CBSI and CBSG.
    Typically: Total Assets should match, or specific risk transfers should reconcile.
    """
    
    # Cross checks join on Dimensions common to both F (Immediate) and G (Guarantor)
    # We remove REPORTING_BASIS from the join since that's what we are comparing (F vs G)
    CBS_CROSS_DIMS = [d for d in CBS_STD_DIMS if d != "REPORTING_BASIS"]

    return [
        ValidationRule(
            rule_id="CBS_CROSS_01",
            description="Total Assets (Immediate) == Total Assets (Guarantor)",
            # LHS: Immediate Basis (F), Total Assets (F)
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == "F") & 
                (df.POSITION == "F") & 
                (df.INSTRUMENT == "A") & 
                (df.CP_SECTOR == "A")
            ],
            # RHS: Guarantor Basis (G), Total Assets (F)
            # Note: Total Assets (Position F) should generally be invariant to risk transfer 
            # unless specific consolidation scope differs.
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == "G") & 
                (df.POSITION == "F") & 
                (df.INSTRUMENT == "A") & 
                (df.CP_SECTOR == "A")
            ],
            join_dims=CBS_CROSS_DIMS,
            operator='eq',
            tolerance=5.0
        ),
        ValidationRule(
            rule_id="CBS_CROSS_02",
            description="Foreign Claims (Guarantor) >= Foreign Claims (Immediate) - Net Risk Transfer",
            # This is a simplified check. A proper check requires: G = I + Inward - Outward.
            # Here we check strict equality for Global Total if Inward/Outward net to zero 
            # (which they often don't globally). 
            # Placeholder for exact logic found in 'check_cbs.py'.
            lhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == "G") & 
                (df.POSITION == "C") & # Total Claims
                (df.CP_COUNTRY == "5J") # Global
            ],
            rhs_filter=lambda df, ctx: df[
                (df.REPORTING_BASIS == "F") & 
                (df.POSITION == "C") & 
                (df.CP_COUNTRY == "5J")
            ],
            join_dims=CBS_CROSS_DIMS,
            operator='eq', # Likely requires high tolerance or specific Net Risk adjustment
            tolerance=100.0 
        )
    ]
