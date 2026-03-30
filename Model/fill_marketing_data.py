import pandas as pd
import numpy as np

df = pd.read_csv("marketing_model_dataset.csv")

# -------------------------------
# Stability handling
# -------------------------------
df['avg_stability'] = df[['eg_stability','sa_stability','ae_stability']].mean(axis=1)

# If still null, fill with global mean
df['avg_stability'] = df['avg_stability'].fillna(df['avg_stability'].mean())

# -------------------------------
# Ratings fill
# -------------------------------
df['imdb_rating'] = df['imdb_rating'].fillna(df['imdb_rating'].mean())
df['elcinema_rating'] = df['elcinema_rating'].fillna(df['elcinema_rating'].mean())
df['letterboxd_rating'] = df['letterboxd_rating'].fillna(df['letterboxd_rating'].mean())
df['letterboxd_votes'] = df['letterboxd_votes'].fillna(df['letterboxd_votes'].mean())

# -------------------------------
# Conversion Rate (business logic)
# -------------------------------
df['conversion_rate'] = (
    0.08
    + 0.015 * (df['imdb_rating'] - 6)
    + 0.02 * (df['letterboxd_rating'] - 3)
    + 0.04 * (df['avg_stability'] - 0.6)
)

df['conversion_rate'] = df['conversion_rate'].clip(0.05, 0.25)

# -------------------------------
# Marketing Spend (realistic)
# -------------------------------
df['total_marketing_spend'] = (
    df['mena_total_admissions'] * np.random.uniform(0.03, 0.07, len(df))
)

# -------------------------------
# First Watch formula
# -------------------------------
df['total_first_watch'] = (
    df['mena_total_admissions'] * df['conversion_rate']
    + df['total_marketing_spend'] * 1.1
)

# Round
df['total_marketing_spend'] = df['total_marketing_spend'].round(0)
df['total_first_watch'] = df['total_first_watch'].round(0)

# Save
df.to_csv("marketing_model_dataset_filled.csv", index=False)

print("Dataset ready: marketing_model_dataset_filled.csv")