import streamlit as st
import pandas as pd
import numpy as np
import pymysql
import pickle
from scipy.sparse import hstack

# -------------------------------------------------------
# 1. Load model + encoder + feature metadata
# -------------------------------------------------------
model = pickle.load(open("model_tuned.pkl", "rb"))
encoder = pickle.load(open("encoder_tuned.pkl", "rb"))
cat_cols = pickle.load(open("cat_cols.pkl", "rb"))
num_cols = pickle.load(open("num_cols.pkl", "rb"))
feature_names = pickle.load(open("feature_names.pkl", "rb"))

# -------------------------------------------------------
# 2. Connect to RDS
# -------------------------------------------------------
def get_connection():
    return pymysql.connect(
        host="amazonsql.cepao6y6sh9u.us-east-1.rds.amazonaws.com",
        user="admin",
        password="amazondb",
        port=3306,
        database="amazon"
    )

# -------------------------------------------------------
# 3. Preprocessing function (matches training pipeline)
# -------------------------------------------------------
def preprocess(df):
    # Columns dropped during training
    drop_cols = [
        "shipment_id", "order_id", "Order_ID", "customer_id", "Customer_ID",
        "Shipment_ID", "Shipping_address_ID", "Payment_ID",
        "tracking_number",
        "error_class", "error_class_desc",
        "reason_code",
        "generated_at",
        "signup_date",
        "Order_date",
        "created_at_x", "created_at_y",
        "updated_at_x", "updated_at_y",
        "Updated_at",
        "date_key", "region"
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # 1. Ensure all categorical columns exist
    for col in cat_cols:
        if col not in df.columns:
            df[col] = "unknown"
        else:
            df[col] = df[col].astype(str)

    # 2. Ensure datetime → numeric
    for col in ["estimated_delivery", "actual_delivery"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].astype("int64") // 1_000_000_000
        else:
            df[col] = 0

    # 3. Ensure all numeric columns exist
    for col in num_cols:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df[num_cols] = df[num_cols].astype("float32")

    # 4. One-hot encode categorical features
    X_cat = encoder.transform(df[cat_cols])
    X_num = df[num_cols].values.astype("float32")

    # 5. Merge → DataFrame
    X_sparse = hstack([X_cat, X_num])
    X_df = pd.DataFrame(X_sparse.toarray(), columns=feature_names)

    # 6. Reindex to guarantee perfect match with training
    X_df = X_df.reindex(columns=feature_names, fill_value=0)

    return X_df

# -------------------------------------------------------
# 4. Streamlit UI
# -------------------------------------------------------
st.title("📦 Shipment Delivery Risk Prediction System")
st.write("Enter a Shipment ID to retrieve its details and predict risk level.")

shipment_id = st.text_input("Shipment ID:")

if shipment_id:
    conn = get_connection()

    query = f"""
        SELECT 
            s.*,
            o.Order_status,
            c.state,
            c.preferred_language,
            c.account_status
        FROM shipments s
        LEFT JOIN orders o ON s.order_id = o.Order_ID
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.shipment_id = '{shipment_id}'
        LIMIT 1;
    """

    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        st.error("❌ Shipment ID not found.")
    else:
        st.subheader("📄 Shipment Details")
        st.dataframe(df)

        # Preprocess input
        X_final = preprocess(df)

        # Predict
        proba = model.predict_proba(X_final)[0]
        pred_class = int(np.argmax(proba))

        st.subheader("🔮 Prediction Result")
        st.write(f"### Predicted Error Class: **{pred_class}**")

        st.write("### Class Probabilities")
        st.json({
            "Class 0 - On Time": float(proba[0]),
            "Class 1 - Minor Delay": float(proba[1]),
            "Class 2 - Major Delay": float(proba[2]),
            "Class 3 - Lost / Severe Failure": float(proba[3])
        })

        st.subheader("📌 Recommended Action")
        if pred_class == 0:
            st.success("Shipment likely on time — no action needed.")
        elif pred_class == 1:
            st.info("Minor delay expected — monitor carrier status.")
        elif pred_class == 2:
            st.warning("Major delay predicted — contact customer proactively.")
        elif pred_class == 3:
            st.error("High risk of loss — initiate contingency workflow immediately.")
