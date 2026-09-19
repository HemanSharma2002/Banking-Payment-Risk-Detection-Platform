from pathlib import Path

import yaml

from src.data_generation.generator import generate_banking_data


def test_generate_banking_data(tmp_path):

    config = {
        "environment": "test",
        "data_generation": {
            "customers": 10,
            "accounts": 20,
            "merchants": 10,
            "transactions": 50,
        },
    }

    config_file = tmp_path / "test_config.yaml"

    with config_file.open("w", encoding="utf-8") as file:
        yaml.safe_dump(config, file)

    generated = generate_banking_data(
        str(config_file)
    )

    assert len(generated["customers"]) == 10
    assert len(generated["accounts"]) == 20
    assert len(generated["merchants"]) == 10
    assert len(generated["transactions"]) == 50