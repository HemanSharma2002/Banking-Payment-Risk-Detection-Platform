from src.common.config import load_config

def test_load_config():
    config = load_config()

    assert config["environment"] == "dev"
    assert config["risk"]["high_value_threshold"] == 100000
    assert config["risk"]["velocity"]["window_minutes"] == 10


def test_data_generation_config():
    config = load_config()

    data_config = config["data_generation"]

    assert data_config["customers"] == 10000
    assert data_config["accounts"] == 15000
    assert data_config["merchants"] == 1000
    assert data_config["transactions"] == 100000