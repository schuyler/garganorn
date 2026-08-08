"""The atgeo containment level vocabulary (pipeline-implementation-decisions.md
"OQ-P2-2 — containment level vocabulary").

LEVEL_VOCAB is the single source of truth mapping Overture division `subtype`
to the atgeo containment `level` integer. It is keyed on subtype alone —
Overture's raw `admin_level` is 96% NULL and ambiguous within a subtype, so
it is never used as an input to this mapping (pipeline-implementation-decisions.md
"OQ-P2-2 — containment level vocabulary").

Two consumers derive from this dict so SQL and Python cannot drift:
  - garganorn/stages.py renders a `${level_case}` CASE expression (with NO
    ELSE branch, belt-and-braces per the level-vocabulary decision above)
    into overture_division_import.sql from LEVEL_VOCAB.
  - garganorn/stages.py's fail-loud validator in stage_division_import()
    checks every division subtype against LEVEL_VOCAB's keys and raises
    RuntimeError listing any unmapped subtypes.

Values follow a uniform stride-5 (per the level-vocabulary decision above):
borough=55, macrohood=60, neighborhood=65, microhood=70. This is a protocol
change from the narrower table in atgeo-spec.md's "Containment levels"
section (which only defined levels through neighborhood=60) — macrohood
and microhood are additions, and
neighborhood moves from 60 to 65.

Level 0 (continent) has no producer entry: continents are never emitted as
division rows, so they never appear as a LEVEL_VOCAB key.
"""

LEVEL_VOCAB: dict[str, int] = {
    "country": 10,
    "dependency": 15,
    "region": 25,
    "county": 35,
    "localadmin": 45,
    "locality": 50,
    "borough": 55,
    "macrohood": 60,
    "neighborhood": 65,
    "microhood": 70,
}


def level_case_sql(subtype_expr: str = "d.subtype", alias: str | None = None) -> str:
    """Render the LEVEL_VOCAB CASE expression for SQL, with NO ELSE branch.

    Single-source rendering of the CASE used to compute the atgeo containment
    `level` from a division `subtype` column (pipeline-implementation-decisions.md
    "OQ-P2-2 — containment level vocabulary"). The
    absence of an ELSE branch is intentional: an unmapped subtype must
    produce NULL so the fail-loud validator and the post-CTAS NULL-level
    assertion in stage_division_import() catch it, rather than silently
    defaulting.

    By default returns the bare CASE expression (no trailing "AS <alias>"),
    matching the `${level_case}` placeholder in overture_division_import.sql,
    which already supplies its own "AS level" suffix. Pass `alias` to append
    "AS <alias>" for callers that need a fully-aliased column expression.

    Args:
        subtype_expr: SQL expression for the subtype column to match against
            (default "d.subtype", matching overture_division_import.sql).
        alias: If given, append "AS <alias>" to the returned expression.

    Returns:
        A string of the form "CASE\\n    WHEN <subtype_expr> = '<k>' THEN <v>\\n    ...\\nEND",
        optionally suffixed with " AS <alias>".
    """
    case_sql = "CASE\n" + "\n".join(
        f"            WHEN {subtype_expr} = '{subtype}' THEN {level}"
        for subtype, level in LEVEL_VOCAB.items()
    ) + "\n        END"
    if alias is not None:
        return f"{case_sql} AS {alias}"
    return case_sql
