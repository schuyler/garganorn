# Planned features

New feature ideas that haven't been designed or scoped yet. Add new
sections here rather than starting another file.

Each section below is its own proposal with its own status. Nothing in this
document is scoped for implementation until its section says so.

## Collection and service metadata

Status: implemented (`org.atgeo.describeService`, `org.atgeo.collection`;
see `pipeline-artifacts.md`'s `stage_export` section). What remains open:

- A DID-based discovery layer, so a client can learn a service *exists and
  what kind it is* before calling any XRPC method on it — resolved via the
  operator's DID, not the service's own HTTP surface. `describeService`
  answers from a DID a client already has; nothing yet gets a client that
  DID in the first place.
- An actionable category/attribute vocabulary, distinct from the current
  displayable one — a client can render `categories`/`attributes` today but
  can't act on them (e.g. filter a query by category) without additional
  per-collection structure.
- Whether summary-band facts (band existence, its top-N cap, its record
  count) belong in `org.atgeo.collection` — deliberately excluded from the
  current record for lack of an approved requirement.

## Two introductory tutorials, in both directions

Status: proposed, not started. No design has been reviewed.

Two audiences will show up at this project, and each finds a different half
of it baffling. Neither is served by the existing documentation, which is
written for people who already accept both sets of premises.

**Geospatial developers arriving at AT Protocol** need to know why the data
looks the way it does. The worked example: coordinates are decimal strings,
not JSON numbers, and every geospatial developer's first instinct is that
this is a mistake. It isn't — AT Protocol's data model has no float type at
all, because records are content-addressed and floats do not reliably
round-trip to identical bytes across architectures, so a re-encoded record
would hash differently and break its own CID. The spec's recommended
workaround for anything that needs a float is exactly what the location
lexicons do: encode it as a string. That one answer opens onto the rest —
what a lexicon is, why records have URIs instead of IDs, what an NSID and a
DID are, and why there is no `/search` endpoint to call.

**AT Protocol developers arriving at geospatial** need the opposite. Why a
bounding box and not a radius; what a quadkey is and why tiles come at mixed
zooms; why longitude comes before latitude; that Web Mercator's projection
breaks down at the poles, and why garganorn's quadkeys reach them anyway;
that the antimeridian is a real place where naive coordinate comparisons
break; what `importance` means and why it isn't comparable across
collections; and the difference between "near me" and "inside this thing,"
which is distance versus containment and wants different data.

Open questions: whether these are documents in this repo, posts on
atgeo.org, or the READMEs of the SDK and a demo app; how much can be carried
by a worked example instead of prose; and whether the AT Protocol half is
better contributed upstream, since none of it is specific to this gazetteer.

## Maritime divisions

Status: proposed, not started. No design has been reviewed.

`garganorn/sql/overture_division_import.sql`'s division import filters on
`is_land=true`, which drops bays, straits, and seas from the division
collection entirely. This is a completeness question, not a data-quality
one — those are real Overture divisions, just not land ones — and it was
deliberately left open rather than decided: is a body of water a useful
containment answer for a client, and if so, does it change tile
assignment or containment-name derivation (see `design-constraints.md`'s
"A record may be referenced by more than one tile")?

## Fold the OSM parquet extraction into the pipeline

Status: proposed, not started. No design has been reviewed.

`scripts/extract-osm-parquet.sh` filters `planet.osm.pbf` with `osmium
tags-filter` and converts the result to parquet, but it lives entirely
outside `garganorn.quadtree`: nothing in the pipeline invokes it, checks its
freshness, or warns when it's stale. Its own cache check (PBF mtime plus a
`filter-selectors.txt` sidecar recording the selector list) is sound, but
nothing wires that check into `quadtree`'s own `--force`, which only
bypasses the pipeline's own stage-level freshness gates. On 2026-08-16 this
let a full `quadtree all --force` run start against a parquet cache dated
March 2026 — five months stale relative to the OSM whitelist expansion
(`39d7c14`) that had just changed the extraction script's selectors — with
nothing in the pipeline's own logs or exit status distinguishing that run
from a correct one.

The idea: have `quadtree` check the extraction script's own freshness
markers before the OSM import stage and either shell out to it automatically
or fail loudly with instructions, rather than silently proceeding on
whatever parquet happens to be on disk.

Timing: the extraction chain includes relation closure alongside the
node/way filter passes and `osmium merge`; there is no current end-to-end
measurement of the whole chain.

Open questions: whether `quadtree` should invoke the script directly
(pulling `osmium`/`osm-pbf-parquet` into the pipeline's dependency surface)
or just check freshness and refuse to proceed; whether the check belongs in
`stage_import` or earlier, before Overture stages run for nothing; and
whether a bbox-scoped build (no full planet PBF on disk) should skip the
check entirely.
