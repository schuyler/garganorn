"""Validation against garganorn's own lexicons.

goat_validate checks a record value with the atproto reference validator
(T1-R1 names goat). validate_def checks an object def (a tile envelope, a
tileset descriptor), which goat cannot address, with lexrpc's
_validate_schema -- the code lexrpc.Server.validate delegates to.
"""
import json
import os
import pathlib
import subprocess
import tempfile

import lexrpc

from garganorn.server import load_lexicons

LEXICON_DIR = pathlib.Path(__file__).resolve().parent.parent / "garganorn" / "lexicon"


def goat_validate(value):
    """Fail unless `goat lex validate` accepts value as a record of its $type."""
    fd, path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f)
        result = subprocess.run(
            ["goat", "lex", "validate", "-c", str(LEXICON_DIR), path],
            capture_output=True, text=True,
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, (
        f"goat rejected {value!r}: {result.stdout}{result.stderr}"
    )


def validate_def(type_name, value):
    """Raise lexrpc's ValidationError unless value validates against the
    object def type_name, e.g. 'org.atgeo.tilePayload' or
    'org.atgeo.defs#tileset'."""
    server = lexrpc.Server(lexicons=load_lexicons())
    server._validate_schema(
        name="output", val=value, type_name=type_name,
        lexicon=type_name.split("#")[0], schema=server.defs[type_name],
    )
