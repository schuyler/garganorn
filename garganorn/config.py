"""Configuration loader for Garganorn."""
import ipaddress
from urllib.parse import urlsplit

import yaml


def _validate_public_base(public_base):
    parts = urlsplit(public_base)
    if parts.scheme == "https":
        return
    host = parts.hostname or ""
    if parts.scheme == "http":
        if host == "localhost" or host.endswith(".localhost"):
            return
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if ip.is_loopback or ip.is_private or ip.is_link_local:
                return
    raise ValueError(
        f"public_base must be https, or http on a development host "
        f"(localhost, *.localhost, or a loopback/private/link-local IP): {public_base}"
    )


def load_config(path):
    """Load a YAML config file and return (repo, tiles_config, public_base)."""
    with open(path) as f:
        config = yaml.safe_load(f)

    repo = config.get("repo", "places.atgeo.org")
    public_base = config.get("public_base", f"https://{repo}").rstrip("/")
    _validate_public_base(public_base)

    return repo, config.get("tiles"), public_base
