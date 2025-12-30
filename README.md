# Universal IBS Validator

A specialized Python library for validating International Banking Statistics (IBS) data submissions for the Bank for International Settlements (BIS). This tool automates the validation of Locational Banking Statistics (LBS) and Consolidated Banking Statistics (CBS) data points, ensuring consistency and accuracy before submission.

## 🚀 Features

* **Universal Applicability:** Designed to be country-agnostic. Configurable for any reporting country (e.g., CA, US, GB) and local currency.
* **Vectorized Validation:** Uses high-performance Pandas operations (merges/group-bys) instead of slow iterative loops, handling large datasets efficiently.
* **Comprehensive Rule Sets:**
    * **LBS:** Validates internal consistency for Residency (LBSR) and Nationality (LBSN) reports, plus cross-report consistency checks.
    * **CBS:** Validates internal consistency for Immediate Counterparty (CBSI) and Guarantor Basis (CBSG), including complex risk transfer logic.
* **Interactive Reporting:** Generates detailed HTML dashboards highlighting failing data points with color-coded diffs.

## 📦 Installation

Clone the repository and install the dependencies:

```bash
git clone [https://github.com/your-username/universal-ibs-validator.git](https://github.com/your-username/universal-ibs-validator.git)
cd universal-ibs-validator
pip install .

For development (running tests):

pip install .[dev]
```

## 🛠️ Usage
This library is designed to be used programmatically within your data pipelines.

# 1. Basic Example
```Python

import pandas as pd
from universal_ibs_validator.engine import IBSValidator
from universal_ibs_validator.domain_types import ValidationContext
from universal_ibs_validator.rules.lbs import get_lbs_cross_report_rules
from universal_ibs_validator.reporting import generate_html_report

# 1. Define Context
context = ValidationContext(
    reporting_country="CA",  # Your Country Code (ISO 2-char)
    currency_code="CAD",     # Your Local Currency
    quarter="2025-Q3"
)

# 2. Load Your Data (From CSV, Parquet, SQL, etc.)
lbsr_df = pd.read_csv("path/to/lbsr_data.csv")
lbsn_df = pd.read_csv("path/to/lbsn_data.csv")

# 3. Initialize Validator
validator = IBSValidator(context)

# 4. Run Rules
# Example: Check consistency between Residency and Nationality reports
cross_rules = get_lbs_cross_report_rules()
validator.validate(lhs_df=lbsr_df, rhs_df=lbsn_df, rules=cross_rules)

# 5. Generate Report
failures = validator.get_failures()
generate_html_report(failures, "validation_report.html")
```

# 2. Available Rule Sets
You can import different rule sets depending on what you need to validate:

```Python

from universal_ibs_validator.rules.lbs import (
    get_lbsr_internal_rules,      # Verify LBSR totals (e.g., Total = Dom + For)
    get_lbsn_internal_rules,      # Verify LBSN totals
    get_lbs_cross_report_rules    # Verify LBSR vs LBSN consistency
)
from universal_ibs_validator.rules.cbs import (
    get_cbs_internal_rules,       # Verify CBSI consistency
    get_cbsg_internal_rules,      # Verify CBSG consistency
    get_cbs_cross_report_rules    # Verify CBSI vs CBSG consistency
)
```

## 📂 Project Structure
```Plaintext

universal-ibs-validator/
├── pyproject.toml                 # Package configuration
├── README.md                      # This file
├── src/
│   └── universal_ibs_validator/
│       ├── __init__.py
│       ├── domain_types.py        # Data classes (ValidationContext, ValidationRule)
│       ├── engine.py              # Core validation logic (Vectorized Merges)
│       ├── reporting.py           # HTML report generation
│       └── rules/
│           ├── __init__.py
│           ├── lbs.py             # LBS Consistency Logic
│           └── cbs.py             # CBS Consistency Logic
└── tests/                         # Unit tests
```

## 📊 Data Requirements
The validator expects Pandas DataFrames with standard BIS SDMX column names:

**LBS Columns:** POSITION, INSTRUMENT, DENOM, CURR_TYPE, PARENT_CTY, REP_BANK_TYPE, REP_CTY, CP_SECTOR, CP_COUNTRY, VALUE

**CBS Columns:** MEASURE, REP_COUNTRY, BANK_TYPE, REPORTING_BASIS, POSITION, INSTRUMENT, REMAINING_MATURITY, CP_CURRENCY, CP_SECTOR, CP_COUNTRY, VALUE

_Note: Column names are case-insensitive (the engine normalizes them to uppercase)._

## 🤝 Contributing
Contributions are welcome! If you find a new BIS validation rule or want to improve the engine performance:

1. Fork the repository.

2. Create a feature branch (git checkout -b feature/NewRule).

3. Commit your changes.

4. Open a Pull Request.

## 📄 License
Distributed under the MIT License. See LICENSE for more information.

**Developed for the global central banking community to streamline BIS International Banking Statistics (IBS) reporting.**
