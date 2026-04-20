# Shipment Delivery Risk Prediction Dashboard
*A Streamlit Application for Real-Time Shipment Risk Forecasting*

## Overview
This application predicts shipment-level delivery risks using a trained multi-class XGBoost model. The app connects directly to an AWS RDS MySQL database, retrieves shipment information, reconstructs the training feature pipeline, and returns predicted risk classes, probability distributions, and recommended operational actions.

This dashboard is the front-end implementation for the New Business Application described in Part 2 of the project report.

## Project Contents
Make sure the following files remain in the same folder:
- app.py
- model_tuned.pkl
- encoder_tuned.pkl
- cat_cols.pkl
- num_cols.pkl
- feature_names.pkl
- README.md

## Installation
Install required Python packages:
```
pip install streamlit pymysql xgboost scipy scikit-learn pandas numpy
```

## Setup Instructions
1. Download the project folder.
2. Unzip it to your desired location.
3. Navigate to the project directory in your terminal:
```
cd "C:\Users\<user_name>\Downloads\app\"
```
4. Launch the Streamlit app:
```
streamlit run app.py
```
The dashboard will automatically open in your browser at http://localhost:8501.

## Using the Application
1. Retrieve a valid shipment ID from MySQL:
```
SELECT shipment_id FROM shipments LIMIT 10;
```
2. Enter the shipment ID into the Streamlit interface.
3. The system will:
- Load shipment/order/customer data from RDS
- Apply the same preprocessing pipeline used during model training
- Predict delivery risk across four classes
- Display probability scores and recommended actions

## Example Output
Predicted Class: 2 (Major Delay)
Probabilities:
  Class 0: 0.00
  Class 1: 0.00
  Class 2: 0.63
  Class 3: 0.37
Recommended Action: Notify customer proactively.

## Troubleshooting
### ModuleNotFoundError: No module named 'xgboost'
Install it:
```
pip install xgboost
```

### ValueError: Feature shape mismatch
Ensure all .pkl files come from the same training run.

### MySQL connection error
Check:
- RDS credentials
- Inbound IP rules

## Notes
This app demonstrates full end-to-end integration of:
- AWS RDS
- Feature engineering
- Multi-class ML inference
- Streamlit UI

It represents the core functionality of the new business application for Part 2.
