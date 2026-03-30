import joblib

model = joblib.load("film_prediction_model.pkl")

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
    X = [[
        eg_adm,
        sa_adm,
        ae_adm,
        imdb,
        elcinema,
        letterboxd_rating,
        letterboxd_votes,
        stability,
        marketing_spend
    ]]
    prediction = model.predict(X)[0]
    return round(prediction)

def predict_required_spend(
    target_first_watch,
    eg_adm,
    sa_adm,
    ae_adm,
    imdb,
    elcinema,
    letterboxd_rating,
    letterboxd_votes,
    stability
):
    coef = model.coef_
    intercept = model.intercept_

    # [eg_adm, sa_adm, ae_adm, imdb, elcinema, letterboxd_rating, letterboxd_votes, stability, marketing_spend]
    a, b, c, d, e, f, g, h, i = coef

    spend = (
        target_first_watch
        - (a*eg_adm + b*sa_adm + c*ae_adm + d*imdb + e*elcinema + f*letterboxd_rating + g*letterboxd_votes + h*stability + intercept)
    ) / i

    return round(spend)

if __name__ == "__main__":
    pred = predict_first_watch(
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
    print("Predicted First Watch:", pred)

    spend_needed = predict_required_spend(
        target_first_watch=200000,
        eg_adm=300000,
        sa_adm=120000,
        ae_adm=30000,
        imdb=7.2,
        elcinema=7.5,
        letterboxd_rating=3.4,
        letterboxd_votes=5000,
        stability=0.65
    )
    print("Required Marketing Spend:", spend_needed)