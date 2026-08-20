"""Overture release resolution: the cache may hold several releases, and a
build must read exactly one of them."""

import pytest

from garganorn.stages import resolve_newest_release

PARTS = ("places", "division", "division_area")


def _make_release(root, name, parts=PARTS):
    """Create one release directory under root, populated with `parts`."""
    release = root / name
    release.mkdir()
    if "places" in parts:
        (release / "part-00000-abc.zstd.parquet").write_text("")
    for sub in ("division", "division_area"):
        if sub in parts:
            (release / sub).mkdir()
            (release / sub / "part-00000-abc.zstd.parquet").write_text("")
    return release


def _patterns(root):
    return [
        f"{root}/*/part-*.parquet",
        f"{root}/*/division/*.parquet",
        f"{root}/*/division_area/*.parquet",
    ]


def test_newest_of_two_complete_releases_wins(tmp_path):
    _make_release(tmp_path, "2026-07-22.0")
    _make_release(tmp_path, "2026-08-19.0")

    resolved = resolve_newest_release(_patterns(tmp_path))

    assert all("2026-08-19.0" in p for p in resolved)
    assert not any("2026-07-22.0" in p for p in resolved)


def test_incomplete_newer_release_is_skipped(tmp_path):
    """A divisions-only newer release must not be selected: it has no places,
    and selecting it would build against zero place rows."""
    _make_release(tmp_path, "2026-07-22.0")
    _make_release(tmp_path, "2026-08-19.0", parts=("division", "division_area"))

    resolved = resolve_newest_release(_patterns(tmp_path))

    assert all("2026-07-22.0" in p for p in resolved)


def test_single_release_resolves_to_itself(tmp_path):
    _make_release(tmp_path, "2026-08-19.0")

    resolved = resolve_newest_release(_patterns(tmp_path))

    assert all("2026-08-19.0" in p for p in resolved)


def test_no_complete_release_raises_naming_candidates(tmp_path):
    _make_release(tmp_path, "2026-07-22.0", parts=("division",))
    _make_release(tmp_path, "2026-08-19.0", parts=("places",))

    with pytest.raises(RuntimeError) as excinfo:
        resolve_newest_release(_patterns(tmp_path))

    message = str(excinfo.value)
    assert "2026-07-22.0" in message and "2026-08-19.0" in message


def test_patterns_without_a_release_segment_pass_through(tmp_path):
    """A config naming files directly has no release layer to resolve, and
    must not have its filename wildcard mistaken for one."""
    patterns = [f"{tmp_path}/overture/*.parquet"]

    assert resolve_newest_release(patterns) == patterns


def test_resolved_patterns_match_only_one_release_on_disk(tmp_path):
    """An unresolved glob spanning two releases expands to both, reading every
    division twice."""
    import glob as glob_module

    _make_release(tmp_path, "2026-07-22.0")
    _make_release(tmp_path, "2026-08-19.0")

    resolved = resolve_newest_release(_patterns(tmp_path))

    division_files = glob_module.glob(resolved[1])
    assert len(division_files) == 1
