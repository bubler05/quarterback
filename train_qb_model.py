import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt

# ---- 1. Load data ----
df = pd.read_csv('final qb stats.csv')

# ---- 2. Split features / label ----
X = df.drop(columns=['Player', 'Label'])
y = df['Label']

# ---- 3. Train / test split ----
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# ---- 4. Model ----
rf = RandomForestClassifier(
    n_estimators=200,
    class_weight='balanced',
    random_state=42
)
rf.fit(X_train, y_train)

# ---- 5. Evaluate ----
print("CONFUSION MATRIX")
print(confusion_matrix(y_test, rf.predict(X_test)))
print("\nCLASSIFICATION REPORT")
print(classification_report(y_test, rf.predict(X_test), digits=3))

# ---- 6. Feature importance ----
importances = pd.Series(rf.feature_importances_, index=X.columns)
importances.sort_values().plot(kind='barh', figsize=(8,6))
plt.title("Feature importance")
plt.tight_layout()
plt.show()
