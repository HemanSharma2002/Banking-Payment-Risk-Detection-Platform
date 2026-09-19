import random


MERCHANT_CATEGORIES = {
    "GROCERY": "LOW",
    "RESTAURANT": "LOW",
    "RETAIL": "LOW",
    "E_COMMERCE": "LOW",
    "TRAVEL": "MEDIUM",
    "FINANCE": "MEDIUM",
    "CRYPTO": "HIGH",
    "GAMBLING": "HIGH",
}

MERCHANT_NAMES = [
    "FreshMart",
    "QuickBite",
    "MegaRetail",
    "ShopZone",
    "TravelWorld",
    "PayFinance",
    "CryptoExchange",
    "LuckyGames",
]

COUNTRIES = [
    "IN",
    "US",
    "GB",
    "AE",
    "SG",
]


def generate_merchants(count: int) -> list[dict]:
    merchants = []

    categories = list(MERCHANT_CATEGORIES.keys())

    for i in range(1, count + 1):
        category = random.choice(categories)

        merchant = {
            "merchant_id": f"MER_{i:06d}",
            "merchant_name": random.choice(MERCHANT_NAMES),
            "merchant_category": category,
            "country": random.choice(COUNTRIES),
            "risk_level": MERCHANT_CATEGORIES[category],
        }

        merchants.append(merchant)

    return merchants