from src.data_generation.customer_generator import generate_customers


def test_generate_customers():
    customers = generate_customers(10)

    assert len(customers) == 10

    assert customers[0]["customer_id"] == "CUST_000001"

    assert customers[0]["first_name"]
    assert customers[0]["last_name"]

    assert customers[0]["country"] in {
        "IN",
        "US",
        "GB",
        "AE",
        "SG",
    }

    assert customers[0]["risk_profile"] in {
        "LOW",
        "MEDIUM",
        "HIGH",
    }