from dataclasses import dataclass, field
from typing import List, Callable, Literal
import pandas as pd

@dataclass
class ValidationContext:
    """
    Dynamic context for validation to avoid hardcoding.
    """
    reporting_country: str  # e.g., 'CA', 'US', 'GB'
    currency_code: str      # e.g., 'CAD', 'USD'
    quarter: str            # e.g., '2025-Q3'

@dataclass
class ValidationRule:
    """
    Defines a single logic gate for IBS validation.
    """
    rule_id: str
    description: str
    
    # Functions that take (DataFrame, Context) and return a filtered DataFrame
    lhs_filter: Callable[[pd.DataFrame, ValidationContext], pd.DataFrame]
    rhs_filter: Callable[[pd.DataFrame, ValidationContext], pd.DataFrame]
    
    # The dimensions (columns) that must align between LHS and RHS
    join_dims: List[str]
    
    # 'equality', 'lhs_gte_rhs' (>=), 'lhs_lte_rhs' (<=)
    operator: Literal['eq', 'gte', 'lte'] = 'eq' 
    
    tolerance: float = 1.0  # Allowable difference due to rounding
