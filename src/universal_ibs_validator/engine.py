import pandas as pd
import numpy as np
from typing import List, Dict
from .domain_types import ValidationRule, ValidationContext

class IBSValidator:
    def __init__(self, context: ValidationContext):
        self.context = context
        self.results = []

    def validate(self, 
                 lhs_df: pd.DataFrame, 
                 rhs_df: pd.DataFrame, 
                 rules: List[ValidationRule]):
        """
        Runs a list of rules against two dataframes (can be the same df for internal checks).
        """
        # normalize columns to upper case to be safe
        lhs_df.columns = [c.upper() for c in lhs_df.columns]
        rhs_df.columns = [c.upper() for c in rhs_df.columns]

        for rule in rules:
            self._process_rule(rule, lhs_df, rhs_df)

    def _process_rule(self, rule: ValidationRule, lhs_raw: pd.DataFrame, rhs_raw: pd.DataFrame):
        # 1. Apply Filters
        try:
            lhs_data = rule.lhs_filter(lhs_raw, self.context)
            rhs_data = rule.rhs_filter(rhs_raw, self.context)
        except Exception as e:
            print(f"Error filtering data for rule {rule.rule_id}: {e}")
            return

        if lhs_data.empty and rhs_data.empty:
            return

        # 2. Aggregation
        # We perform a groupby sum to ensure unique keys before merging
        lhs_agg = lhs_data.groupby(rule.join_dims)['VALUE'].sum().reset_index()
        rhs_agg = rhs_data.groupby(rule.join_dims)['VALUE'].sum().reset_index()

        lhs_agg = lhs_agg.rename(columns={'VALUE': 'LHS_VALUE'})
        rhs_agg = rhs_agg.rename(columns={'VALUE': 'RHS_VALUE'})

        # 3. Vectorized Merge (Full Outer Join)
        merged = pd.merge(lhs_agg, rhs_agg, on=rule.join_dims, how='outer').fillna(0)

        # 4. Calculate Logic
        merged['DIFF'] = merged['LHS_VALUE'] - merged['RHS_VALUE']
        
        # Determine failure based on operator
        if rule.operator == 'eq':
            # Fail if abs(diff) > tolerance
            merged['IS_FAIL'] = merged['DIFF'].abs() > rule.tolerance
            operator_symbol = "="
        elif rule.operator == 'gte':
            # Fail if LHS < RHS (allowing for tolerance)
            # Effectively: LHS - RHS < -tolerance
            merged['IS_FAIL'] = merged['DIFF'] < -rule.tolerance
            operator_symbol = ">="
        elif rule.operator == 'lte':
            # Fail if LHS > RHS
            # Effectively: LHS - RHS > tolerance
            merged['IS_FAIL'] = merged['DIFF'] > rule.tolerance
            operator_symbol = "<="

        failures = merged[merged['IS_FAIL']].copy()

        if not failures.empty:
            failures['RULE_ID'] = rule.rule_id
            failures['DESCRIPTION'] = rule.description
            failures['OPERATOR'] = operator_symbol
            # Add context for reporting
            for dim in rule.join_dims:
                failures[dim] = failures[dim].astype(str)
            
            self.results.append(failures)

    def get_failures(self) -> pd.DataFrame:
        if not self.results:
            return pd.DataFrame()
        return pd.concat(self.results, ignore_index=True)
