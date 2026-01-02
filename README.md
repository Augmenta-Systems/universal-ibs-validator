# 📐 Universal IBS Validator (The Logic Engine)

![Role](https://img.shields.io/badge/Role-Logic%20Engine-purple)
![Domain](https://img.shields.io/badge/Domain-BIS%20Statistics-green)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)

**The Mathematical & Statistical Brain behind the Augmenta Sovereign Shield.**

---

### 🧠 Overview
The **Universal IBS Validator** is a high-performance Python library designed to validate International Banking Statistics (IBS).

While traditional validators only output "Pass/Fail" reports, this engine is designed for **Active Governance**. It acts as the "Integrity & Confidentiality" engine for the **Triple-Lock Governance Model**:
1.  **Validates Math:** Checks complex identities (e.g., $B = I + J + M$).
2.  **Calculates Dominance:** Detects statistical confidentiality breaches (e.g., "Dominance Rule" > 60% share).
3.  **Tags Data:** Enriches datasets with `QUALITY_STATUS` and `CONFIDENTIALITY_STATUS` metadata for downstream security enforcement.

---

### ⚡ Key Features

#### 1. Statistical Confidentiality (The Dominance Rule)
Automatically detects if a data point is "too identifying" to be published.
* **Logic:** Calculates if a single contributor (e.g., a specific Bank) holds >60% of a reported aggregate.
* **Output:** Tags rows as `N` (Not Free to Publish) or `F` (Free).
* **Use Case:** Allows "World Totals" to be published while automatically hiding sensitive country-level contributors.

#### 2. Vectorized Validation (The Integrity Check)
Uses high-performance Pandas operations to validate millions of rows in seconds without loops.
* **LBSR / LBSN:** Residency and Nationality internal consistency.
* **Cross-Report:** Verifies that Assets (Residency) match Assets (Nationality).

#### 3. Enrichment Mode
Designed to run inside ETL pipelines (Databricks/Synapse). Instead of stopping the pipeline on error, it **tags** the data so the Governance Shield can hide invalid rows from analysts while preserving them for engineers.

---

### 🛠️ Usage

#### A. Basic Validation (Reporting)
Generate an HTML report of failures for data stewards.

```python
from universal_ibs_validator.engine import IBSValidator
from universal_ibs_validator.reporting import generate_html_report

validator = IBSValidator(context)
validator.validate(df_lbsr, df_lbsn, rules)
generate_html_report(validator.get_failures(), "report.html")
```

#### B. The "Sovereign Pipeline" (Tagging)
Enrich data for the Triple-Lock Governance Shield.

```Python
from universal_ibs_validator.confidentiality import apply_dominance_rule
from universal_ibs_validator.engine import IBSValidator

# 1. Apply Confidentiality Logic
# Flags 'N' if any bank holds >60% of the market
df_tagged = apply_dominance_rule(
    raw_df, 
    value_col="VALUE", 
    group_cols=["REP_CTY", "SECTOR"], 
    contributor_col="BANK_ID"
)

# 2. Apply Integrity Logic
# Flags 'FAIL' if math identities are broken
validator = IBSValidator(context)
df_final = validator.tag_dataset(df_tagged, rules=get_rules())

# 3. Output for Shield Enforcement
df_final.write.saveAsTable("gold_sovereign_data")
```

#### 📂 Project Structure
```Plaintext
universal-ibs-validator/
├── src/universal_ibs_validator/
│   ├── rules/                 # BIS Validation Logic (LBS/CBS)
│   ├── confidentiality.py     # Dominance Rule Logic (The "N" vs "F" Calculator)
│   ├── engine.py              # Core Vectorized Engine & Tagging System
│   └── domain_types.py        # Data Classes
│
├── tests/
│   └── test_confidentiality.py # Unit tests for Dominance Logic
│
└── README.md
```

#### 🤝 Integration
This library provides the **Metadata Tags** required by the _**augmenta-governance-shield**_ to enforce Row-Level Security.

#### 📄 License
Distributed under the MIT License.
