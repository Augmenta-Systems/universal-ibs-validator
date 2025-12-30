import pandas as pd
from src.universal_ibs_validator.engine import IBSValidator
from src.universal_ibs_validator.domain_types import ValidationContext
from src.universal_ibs_validator.rules.lbs import get_lbs_consistency_rules
from src.universal_ibs_validator.rules.cbs import get_cbs_internal_rules
from src.universal_ibs_validator.reporting import generate_html_report

def main():
    # 1. Setup Context (This makes it universal)
    context = ValidationContext(
        reporting_country="CA",  # Change this for other countries
        currency_code="CAD",
        quarter="2025-Q3"
    )

    # 2. Load Data 
    # In a real scenario, load from CSV/Parquet/SQL
    # Here we mock data to demonstrate a failure
    print("Loading data...")
    
    # Mock LBSR (Residency) Data
    lbs_r_data = pd.DataFrame({
        'POSITION': ['C'], 'INSTRUMENT': ['A'], 'DENOM': ['TO1'], 
        'CURR_TYPE': ['T'], 'REP_CTY': ['CA'], 'CP_SECTOR': ['B'], 'CP_COUNTRY': ['US'],
        'PARENT_CTY': ['5J'], 'REP_BANK_TYPE': ['D'], 
        'VALUE': [100.0]
    })

    # Mock LBSN (Nationality) Data
    # Deliberately mismatched value (90.0 vs 100.0) to trigger LBS_CC24 failure
    lbs_n_data = pd.DataFrame({
        'POSITION': ['C'], 'INSTRUMENT': ['A'], 'DENOM': ['TO1'], 
        'CURR_TYPE': ['T'], 'REP_CTY': ['CA'], 'CP_SECTOR': ['B'], 'CP_COUNTRY': ['US'],
        'PARENT_CTY': ['CA'], # Matches context.reporting_country
        'REP_BANK_TYPE': ['A'], 
        'VALUE': [90.0] 
    })

    # 3. Initialize Validator
    validator = IBSValidator(context)

    # 4. Run LBS Checks
    print("Running LBS Consistency Checks...")
    lbs_rules = get_lbs_consistency_rules()
    validator.validate(lbs_r_data, lbs_n_data, lbs_rules)

    # 5. Run CBS Checks (Internal consistency)
    # Using dummy data for CBS just to show it runs
    cbs_data = pd.DataFrame({
        'MEASURE': ['A'], 'REP_COUNTRY': ['CA'], 'BANK_TYPE': ['CA'], 'REPORTING_BASIS': ['F'],
        'POSITION': ['B'], 'INSTRUMENT': ['A'], 'REMAINING_MATURITY': ['A'], 
        'CP_SECTOR': ['A'], 'CP_COUNTRY': ['US'],
        'CP_CURRENCY': ['FC1'], 'VALUE': [500.0] # LHS
    })
    # Append RHS row that is SMALLER (400) to trigger CBS_CC08 (LHS <= RHS) failure
    cbs_data = pd.concat([cbs_data, pd.DataFrame({
        'MEASURE': ['A'], 'REP_COUNTRY': ['CA'], 'BANK_TYPE': ['CA'], 'REPORTING_BASIS': ['F'],
        'POSITION': ['I'], 'INSTRUMENT': ['A'], 'REMAINING_MATURITY': ['A'], 
        'CP_SECTOR': ['A'], 'CP_COUNTRY': ['US'],
        'CP_CURRENCY': ['TO1'], 'VALUE': [400.0] # RHS
    })])
    
    print("Running CBS Internal Checks...")
    cbs_rules = get_cbs_internal_rules()
    validator.validate(cbs_data, cbs_data, cbs_rules)

    # 6. Generate Report
    failures = validator.get_failures()
    generate_html_report(failures, "my_ibs_validation.html")

if __name__ == "__main__":
    main()
