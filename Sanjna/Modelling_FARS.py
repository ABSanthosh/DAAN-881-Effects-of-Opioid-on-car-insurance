import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve

# -------------------------------
# 1. Load and preprocess data
# -------------------------------
df = pd.read_csv('/content/FARS_combined__cleaned_2013_2023.csv')  # Adjust path as needed

# Convert crash_date to datetime if not already
df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')

# Map severity_level if it's still string-based
severity_mapping = {
    "Property Damage Only": 0,
    "Fatal": 1,
    "Suspected Serious Injury": 2,
    "Suspected Minor Injury": 3,
    "Possible Injury": 4,
    "Unknown if Injured": 9,
    "Injury – Unknown Severity": 8
}
if df['severity_level'].dtype == 'object':
    df['severity_level'] = df['severity_level'].map(severity_mapping).fillna(9)

# -------------------------------
# 2. Select features and target
# -------------------------------
features = [
    'state', 'fatalities', 'injuries', 'severity_level',
    'young_driver_flag', 'mature_driver_flag',
    'driver_age', 'driver_sex', 'RACE'
]

target = 'opioid_flag'

# Drop missing values from relevant columns
df_model = df[features + [target]].dropna()

X = df_model[features]
y = df_model[target]

# -------------------------------
# 3. Train-test split
# -------------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, stratify=y, random_state=42
)

# -------------------------------
# 4. Train Random Forest (balanced)
# -------------------------------
rf_model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    class_weight='balanced',
    random_state=42
)
rf_model.fit(X_train, y_train)

# -------------------------------
# 5. Predict and Evaluate
# -------------------------------
y_pred = rf_model.predict(X_test)
y_proba = rf_model.predict_proba(X_test)[:, 1]

print("Classification Report:")
print(classification_report(y_test, y_pred, digits=4))

# -------------------------------
# 6. Confusion Matrix
# -------------------------------
cm = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(6, 5))
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=True, linewidths=0.5)
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.title('Confusion Matrix (Random Forest - Opioid Prediction)')
plt.xticks(ticks=[0.5, 1.5], labels=["No Opioid", "Opioid"], rotation=0)
plt.yticks(ticks=[0.5, 1.5], labels=["No Opioid", "Opioid"], rotation=0)
plt.tight_layout()
plt.show()

# -------------------------------
# 7. ROC Curve
# -------------------------------
fpr, tpr, _ = roc_curve(y_test, y_proba)
auc_score = roc_auc_score(y_test, y_proba)

plt.figure(figsize=(8, 6))
plt.plot(fpr, tpr, label=f'ROC Curve (AUC = {auc_score:.4f})', color='darkorange')
plt.plot([0, 1], [0, 1], linestyle='--', color='gray')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC Curve - Random Forest (Opioid Involvement)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
