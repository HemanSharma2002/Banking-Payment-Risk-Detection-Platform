from src.common.config import load_config

def test_load_config():
    config = load_config()

    assert config["environment"] == "dev"
    assert config["risk"]["high_value_threshold"] == 100000
    assert config["risk"]["velocity"]["window_minutes"] == 10