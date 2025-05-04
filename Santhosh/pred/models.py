import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix, roc_curve, precision_recall_curve

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

# ========== FUNCTION: Prepare Data ==========
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

# ========== FUNCTION: Plot Utilities ==========
def plot_confusion_matrix(y_true, y_pred, title, path):
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(6,4))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
    plt.title(title)
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_roc_curve(y_true, y_score, title, path):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    plt.figure(figsize=(6,4))
    plt.plot(fpr, tpr, label=f"AUC: {roc_auc_score(y_true, y_score):.2f}")
    plt.plot([0,1], [0,1], linestyle='--')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()

# ========== FUNCTION: Driver-Level Prediction ==========
def driver_level_prediction(df, dbPrefix):
    features = ['year', 'driver_age', 'driver_sex', 'severity_level']
    X = df[features].fillna(-1)
    y = df['opioid_flag']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    models = {
        'RandomForest': RandomForestClassifier(n_estimators=100, random_state=42),
        'LogisticRegression': LogisticRegression(max_iter=1000, random_state=42),
        'XGBoost': XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42)
    }

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_score = model.predict_proba(X_test)[:,1]

        report = classification_report(y_test, y_pred, output_dict=True)
        auc = roc_auc_score(y_test, y_score)

        with open(f"./scripts/pred/statistics/{dbPrefix}_driver_level_{name}_report2.txt", "w") as f:
            f.write(f"AUC: {auc}\n")
            f.write(pd.DataFrame(report).transpose().to_string())

        plot_confusion_matrix(y_test, y_pred, f"{dbPrefix.upper()} Driver Level {name} Confusion Matrix", f"./scripts/pred/figures/{dbPrefix}_driver_level_{name}_confusion2.png")
        plot_roc_curve(y_test, y_score, f"{dbPrefix.upper()} Driver Level {name} ROC Curve", f"./scripts/pred/figures/{dbPrefix}_driver_level_{name}_roc2.png")

# ========== FUNCTION: County-Level Prediction ==========
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

    if county_summary.empty:
        print(f"No data for {dbPrefix.upper()} county prediction.")
        return

    dynamic_threshold = min(0.10, (county_summary['opioid_crashes'].sum() / county_summary['total_crashes'].sum()))
    county_summary['opioid_risk'] = (county_summary['opioid_crashes'] / county_summary['total_crashes']) > dynamic_threshold

    features = ['year', 'avg_driver_age', 'prop_male', 'fatal_crash_rate', 'serious_injury_rate', 'avg_severity_score']
    X = county_summary[features].fillna(-1)
    y = county_summary['opioid_risk'].astype(int)

    if y.nunique() == 1:
        print(f"Only one class present for {dbPrefix.upper()} county prediction. Skipping models.")
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    models = {
        'RandomForest': RandomForestClassifier(n_estimators=100, random_state=42),
        'LogisticRegression': LogisticRegression(max_iter=1000, random_state=42),
        'XGBoost': XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42)
    }

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_score = model.predict_proba(X_test)[:,1]

        report = classification_report(y_test, y_pred, output_dict=True)
        auc = roc_auc_score(y_test, y_score)

        with open(f"./scripts/pred/statistics/{dbPrefix}_county_level_{name}_report2.txt", "w") as f:
            f.write(f"AUC: {auc}\n")
            f.write(pd.DataFrame(report).transpose().to_string())

        plot_confusion_matrix(y_test, y_pred, f"{dbPrefix.upper()} County Level {name} Confusion Matrix", f"./scripts/pred/figures/{dbPrefix}_county_level_{name}_confusion2.png")
        plot_roc_curve(y_test, y_score, f"{dbPrefix.upper()} County Level {name} ROC Curve", f"./scripts/pred/figures/{dbPrefix}_county_level_{name}_roc2.png")

# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    for prefix in ['tn', 'pa', 'sc']:
        df = prepare_predictive_data(prefix)
        driver_level_prediction(df, prefix)
        county_level_prediction(df, prefix)

