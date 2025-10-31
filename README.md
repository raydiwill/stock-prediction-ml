# Daily Stock Prediction with Machine Learning  
**Status:** Early-stage — Data ingestion working, modeling & production not yet started  

A portfolio project that pulls end-of-day (EOD) stock data from the MarketStack API, processes it, and sets the foundation for building a full machine-learning prediction pipeline.  
Current milestone: *data collection & processing*.  
Next milestones: feature engineering → model training → evaluation → deployment.

---

## ⚙️ What it does (current functionality)  
- Fetches EOD stock data via MarketStack API.  
- Converts JSON responses into a tidy `pandas` DataFrame.  
- Saves cleaned data to `data/raw/<filename>.parquet`.  
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
│ │ │ ├─ init.py
│ │ │ └─ pull.py # fetch + process + save logic
│ │ ├─ features/ # placeholder for feature-engineering scripts
│ │ └─ init.py
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
