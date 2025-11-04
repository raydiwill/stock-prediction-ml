# Daily Stock Prediction with Machine Learning  
**Status:** Data ingestion & validation complete — EOD data ingestion working and validated with Great Expectations.

A portfolio project that pulls end-of-day (EOD) stock data from the MarketStack API, processes it, validates the dataset, and sets the foundation for building a full machine-learning prediction pipeline.  
Current milestone: *data collection & validation*.  
Next milestones: feature engineering → model training → evaluation → deployment.

---

## ⚙️ What it does (current functionality)  
- Fetches EOD stock data via MarketStack API.  
- Converts JSON responses into a tidy `pandas` DataFrame.  
- Saves cleaned data to `data/raw/<filename>.parquet`.  
- Validates the saved Parquet using Great Expectations (schema, types, uniqueness, ranges).  
- Ready for feature engineering, training, and downstream modeling.

---

## 📁 Project layout  
```bash
├─ data/
│ └─ raw/ # output directory for Parquet files
├─ notebooks/
├─ src/
│ └─ stock_prediction_ml/
│ │ ├─ marketstack/
│ │ │ ├─ __init__.py
│ │ │ └─ pull.py # fetch + process + save logic
│ │ ├─ data_validation/
│ │ │ └─ validation.py # Great Expectations validation script
│ │ ├─ features/ # placeholder for feature-engineering scripts
│ │ └─ __init__.py
├─ tests/ # unit tests for modules
├─ .gitignore
├─ .python-version
├─ config.env.example
├─ pyproject.toml
├─ uv.lock
└─ README.md
```

## 🧮 Prerequisites  
- Python 3.13 (or newer)  
- A MarketStack API key (free tier)  
- Recommended: `uv` tool for dependency / environment management  

## 🧰 Installation & Setup  
```bash
# Clone repo
git clone https://github.com/raydiwill/stock-prediction-ml.git
cd stock-prediction-ml

# Use uv to set up environment & dependencies
uv sync
uv run pip install -e .
```

---
