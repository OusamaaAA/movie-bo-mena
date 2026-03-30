import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import joblib

# Load dataset
df = pd.read_csv("marketing_model_dataset_filled.csv")

# Fill missing values
df = df.fillna({
    'eg_total_admissions': 0,
    'sa_total_admissions': 0,
    'ae_total_admissions': 0,
    'imdb_rating': df['imdb_rating'].mean(),
    'elcinema_rating': df['elcinema_rating'].mean(),
    'letterboxd_rating': df['letterboxd_rating'].mean(),
    'letterboxd_votes': df['letterboxd_votes'].mean(),
    'avg_stability': df['avg_stability'].mean(),
    'total_marketing_spend': df['total_marketing_spend'].mean(),
    'total_first_watch': df['total_first_watch'].mean()
})

# Features
X = df[[
    'eg_total_admissions',
    'sa_total_admissions',
    'ae_total_admissions',
    'imdb_rating',
    'elcinema_rating',
    'letterboxd_rating',
    'letterboxd_votes',
    'avg_stability',
    'total_marketing_spend'
]]

# Target
y = df['total_first_watch']

# Train model
model = LinearRegression()
model.fit(X, y)

# Save model
# Save model safely
joblib.dump(model, "film_prediction_model.pkl", compress=3)

print("Model trained and saved as film_prediction_model.pkl")

# Print coefficients
for name, coef in zip(X.columns, model.coef_):
    print(name, ":", coef)

print("Intercept:", model.intercept_)

# Accuracy
y_pred = model.predict(X)
print("R2 Score:", r2_score(y, y_pred))