from src.data_generation.merchant_generator import generate_merchants


def test_generate_merchants():

    merchants = generate_merchants(20)

    assert len(merchants) == 20

    assert merchants[0]["merchant_id"] == "MER_000001"

    valid_categories = {
        "GROCERY",
        "RESTAURANT",
        "RETAIL",
        "E_COMMERCE",
        "TRAVEL",
        "FINANCE",
        "CRYPTO",
        "GAMBLING",
    }

    valid_risk_levels = {
        "LOW",
        "MEDIUM",
        "HIGH",
    }

    valid_countries = {
        "IN",
        "US",
        "GB",
        "AE",
        "SG",
    }

    for merchant in merchants:

        assert merchant["merchant_category"] in valid_categories

        assert merchant["risk_level"] in valid_risk_levels

        assert merchant["country"] in valid_countries