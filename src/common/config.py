from pathlib import Path
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config(config_path: str = "config/config.yaml") -> dict:
    path = PROJECT_ROOT / config_path

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)
    
     
def load_risk_config(
    config_path: str = "config/config.yaml",
) -> dict:

    path = PROJECT_ROOT / config_path

    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["risk"]