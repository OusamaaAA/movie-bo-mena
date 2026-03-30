import joblib
import pandas as pd

model = joblib.load("film_prediction_model.pkl")

# -----------------------------
# Prediction
# -----------------------------
def predict_first_watch(
    eg_adm,
    sa_adm,
    ae_adm,
    imdb,
    elcinema,
    letterboxd_rating,
    letterboxd_votes,
    stability,
    marketing_spend
):
    X = pd.DataFrame([{
        'eg_total_admissions': eg_adm,
        'sa_total_admissions': sa_adm,
        'ae_total_admissions': ae_adm,
        'imdb_rating': imdb,
        'elcinema_rating': elcinema,
        'letterboxd_rating': letterboxd_rating,
        'letterboxd_votes': letterboxd_votes,
        'avg_stability': stability,
        'total_marketing_spend': marketing_spend
    }])

    return model.predict(X)[0]


# -----------------------------
# Business Metrics
# -----------------------------
def calculate_roi(first_watch, marketing_spend):
    return first_watch / marketing_spend


def revenue_estimate(first_watch):
    # Example assumption:
    # Each first watch = $1.2 revenue
    return first_watch * 1.2


def profit_estimate(first_watch, marketing_spend):
    return revenue_estimate(first_watch) - marketing_spend


# -----------------------------
# Decision Function
# -----------------------------
def film_investment_decision(
    eg_adm,
    sa_adm,
    ae_adm,
    imdb,
    elcinema,
    letterboxd_rating,
    letterboxd_votes,
    stability,
    marketing_spend
):
    fw = predict_first_watch(
        eg_adm,
        sa_adm,
        ae_adm,
        imdb,
        elcinema,
        letterboxd_rating,
        letterboxd_votes,
        stability,
        marketing_spend
    )

    roi = calculate_roi(fw, marketing_spend)
    revenue = revenue_estimate(fw)
    profit = profit_estimate(fw, marketing_spend)

    if roi > 4:
        decision = "STRONG BUY"
    elif roi > 2.5:
        decision = "BUY"
    elif roi > 1.5:
        decision = "RISKY"
    else:
        decision = "DO NOT BUY"

    return round(fw), round(roi, 2), round(revenue), round(profit), decision


# -----------------------------
# Example Test
# -----------------------------
if __name__ == "__main__":
    fw, roi, revenue, profit, decision = film_investment_decision(
        eg_adm=300000,
        sa_adm=120000,
        ae_adm=30000,
        imdb=7.2,
        elcinema=7.5,
        letterboxd_rating=3.4,
        letterboxd_votes=5000,
        stability=0.65,
        marketing_spend=40000
    )

    print("First Watch:", fw)
    print("ROI:", roi)
    print("Revenue:", revenue)
    print("Profit:", profit)
    print("Decision:", decision)