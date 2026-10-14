"""The atgeo tile envelope.

Used by garganorn.stages' export path, so there is exactly one
implementation of the envelope shape.

Imports nothing from garganorn — no import cycle with stages.py.
"""
import json


def build_tile_payload(published_at: str, attribution: list, records_json: list) -> bytes:
    """Build the tile payload: {publishedAt, attribution, records}.

    records_json holds record JSON strings (DuckDB's to_json() output); they
    are spliced in verbatim by string composition, not parsed and
    reserialized, which preserves DuckDB's UTF-8 output exactly.
    """
    header = json.dumps({"publishedAt": published_at, "attribution": attribution})
    payload = header[:-1] + ',"records":[' + ",".join(records_json) + ']}'
    return payload.encode("utf-8")
