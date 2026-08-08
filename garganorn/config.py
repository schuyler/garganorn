"""Configuration loader for Garganorn."""
import yaml

def load_config(path):
    """Load a YAML config file and return (repo, tiles_config)."""
    with open(path) as f:
        config = yaml.safe_load(f)

    repo = config.get("repo", "places.atgeo.org")

    return repo, config.get("tiles")
