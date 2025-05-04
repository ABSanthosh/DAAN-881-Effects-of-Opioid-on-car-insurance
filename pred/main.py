import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score

# Ensure directories exist
os.makedirs("./scripts/pred/figures", exist_ok=True)
os.makedirs("./scripts/pred/statistics", exist_ok=True)

# ========== DATABASE CONNECTION ==========
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Ranchero0',
}

def connect_database(DB_NAME):
    url = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{DB_NAME}"
    engine = create_engine(url, pool_recycle=3600, future=True)
    return engine

# ========== FUNCTION 1: Prepare Data ==========
def prepare_predictive_data(dbPrefix):
    engine = connect_database(f"{dbPrefix}_crash_db")
    query = text(f"""
        SELECT 
            YEAR(crash_date) AS year, 
            county_name, 
            severity_level,
            driver_age,
            driver_sex,
            opioid_flag
        FROM {dbPrefix}_processed
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df

# ========== FUNCTION 2: Driver-Level Prediction ==========
def driver_level_prediction(df, dbPrefix):
    features = ['year', 'driver_age', 'driver_sex', 'severity_level']
    X = df[features].fillna(-1)
    y = df['opioid_flag']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred, output_dict=True)

    try:
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:,1])
    except IndexError:
        auc = "AUC not available (only one class present)"

    # Save classification report
    with open(f"./scripts/pred/statistics/{dbPrefix}_driver_level_report.txt", "w") as f:
        f.write(f"AUC: {auc}\n")
        f.write(pd.DataFrame(report).transpose().to_string())

    # Feature importance plot
    feature_importance = pd.Series(model.feature_importances_, index=features)
    plt.figure(figsize=(8,6))
    sns.barplot(x=feature_importance.values, y=feature_importance.index)
    plt.title(f"{dbPrefix.upper()} - Driver Level Feature Importance")
    plt.xlabel("Importance")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"./scripts/pred/figures/{dbPrefix}_driver_level_feature_importance.png")
    plt.close()

# ========== FUNCTION 3: County-Level Prediction ==========
def county_level_prediction(df, dbPrefix):
    county_summary = df.groupby(['county_name', 'year']).agg(
        total_crashes=('opioid_flag', 'count'),
        opioid_crashes=('opioid_flag', 'sum'),
        avg_driver_age=('driver_age', 'mean'),
        prop_male=('driver_sex', lambda x: (x==0).mean()),
        fatal_crash_rate=('severity_level', lambda x: (x==1).mean()),
        serious_injury_rate=('severity_level', lambda x: (x==2).mean()),
        avg_severity_score=('severity_level', 'mean')
    ).reset_index()
    print(f"County for {dbPrefix.upper()} has {len(county_summary)} records.")

    if county_summary.empty:
        print(f"No data for {dbPrefix.upper()} county prediction.")
        return

    # county_summary['opioid_risk'] = (county_summary['opioid_crashes'] / county_summary['total_crashes']) > 0.15

    dynamic_threshold = county_summary['opioid_crashes'].sum() / county_summary['total_crashes'].sum()
    dynamic_threshold = min(0.10, dynamic_threshold)  # force at most 10% if data is very clean
    print(f"Dynamic threshold for {dbPrefix.upper()} is {dynamic_threshold:.3f}")
    county_summary['opioid_risk'] = (county_summary['opioid_crashes'] / county_summary['total_crashes']) > dynamic_threshold

    features = ['year', 'avg_driver_age', 'prop_male', 'fatal_crash_rate', 'serious_injury_rate', 'avg_severity_score']
    X = county_summary[features].fillna(-1)
    y = county_summary['opioid_risk'].astype(int)

    if y.nunique() == 1:
        print(f"Only one class present for {dbPrefix.upper()} county prediction. Skipping AUC.")
        auc = "AUC not available (only one class present)"
    else:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        print(f"Model training for {dbPrefix.upper()} county prediction.")
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        print(f"Model prediction for {dbPrefix.upper()} county prediction.")
        report = classification_report(y_test, y_pred, output_dict=True)
        print(f"Model evaluation for {dbPrefix.upper()} county prediction.")
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:,1])
        print(f"AUC for {dbPrefix.upper()} county prediction: {auc}")
        
        with open(f"./scripts/pred/statistics/{dbPrefix}_county_level_report.txt", "w") as f:
            f.write(f"AUC: {auc}\n")
            f.write(pd.DataFrame(report).transpose().to_string())
            f.write("\n\nPredicted Probabilities:\n")
            proba = model.predict_proba(X_test)
            proba_df = pd.DataFrame(proba, columns=["No Risk", "High Risk"])
            f.write(proba_df.to_string(index=False))

        feature_importance = pd.Series(model.feature_importances_, index=features)
        plt.figure(figsize=(8,6))
        sns.barplot(x=feature_importance.values, y=feature_importance.index)
        plt.title(f"{dbPrefix.upper()} - County Level Feature Importance")
        plt.xlabel("Importance")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"./scripts/pred/figures/{dbPrefix}_county_level_feature_importance.png")
        plt.close()

# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    for prefix in ['pa', 'sc']:
        df = prepare_predictive_data(prefix)
        driver_level_prediction(df, prefix)
        print(f"Starting county level prediction for {prefix.upper()}")
        county_level_prediction(df, prefix)
        print(f"Completed county level prediction for {prefix.upper()}")

