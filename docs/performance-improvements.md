# Performance improvements

Proposed performance work on code that already works correctly —
open-ended, one section per idea, each with its own status. This doc
holds performance-improvement ideas generally, not just containment; add
new sections here rather than starting another file. Nothing here is
scoped for implementation until its section says so.

## Division tile-reference fan-out from garbage geometry

Status: open question, not scoped for implementation.

`stage_division_tile_references` derives a division's tile references
directly from its covering leaves. A garbage geometry
(`known-data-quality-issues.md` documents them) yields a garbage
covering, and the stage reads references straight off it, so it can
still fan out into many tiles silently. Concrete Discovery cases:
Antarctica (71 cells at z4) and a level-50 division reaching 27 cells at
z7 against a level-50 median of 1. Nothing currently bounds or reports
it.

## `COVER_MIN_LEAF_ZOOM` is unmeasured

Status: open question, not scoped for implementation.

The value 12 was inherited from the pre-split `COVER_MAX_ZOOM`, not
measured against real data. It trades edge-join fan-out against stored
fragment count; a real run's `per_level` stats and measured artifact
size are what would justify moving it either way. A floor of 12 holds
edge-join fan-out at what the pre-split covering already produced, so the
unmeasured value is a known-working default rather than a latent risk.

## Which pipeline disk writes earn their keep

Status: one settled note kept as context (the export staging write).
Not scoped for implementation.

The bar is that spill stays bounded — a few dozen GB is fine, hundreds
is not.

### The export staging write

The batched export writes a staging parquet of the payload, estimated at
25–40 GiB. A few dozen GB is within budget, so this is recorded as
understood rather than as work to do.

Zero is reachable for `overture_place` and `osm` by filtering both sides
of the export join on a quadkey prefix range instead of materialising:
for those sources `tile_qk` is always a prefix of the place's own
`qk17`, so both sides prune by zone map and nothing extra is written.
It does not generalise to divisions, whose tile references come from the
covering artifact rather than their own `qk17`, so a places-side `qk17`
filter would drop them silently. That buys back a few dozen GB at the
cost of a second export mechanism — not a trade worth making unless
something else motivates it.
