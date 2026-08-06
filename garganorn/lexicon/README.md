# ATGeo / Garganorn lexicons

Proposed Lexicon schemas for geographic place data on AT Protocol. These are
experimental; interfaces may change. The `community.lexicon.location.*`
schemas mirror (and in some cases extend) the Lexicon Community location
types; `org.atgeo.*` schemas are ATGeo-specific.

Schemas are served by Garganorn at `/<nsid>` and used to shape gazetteer
records and XRPC methods.

## How the pieces fit together

```
community.lexicon.location.*     ← shared “where is it?” encodings
            │
            ▼
      org.atgeo.place            ← gazetteer record (name, locations, attrs, relations)
            │
            ├──────────────────┐
            ▼                  ▼
 org.atgeo.searchRecords   com.atproto.repo.getRecord
 (spatial / text search)   (single-record fetch)
            │
            ▼
 org.atgeo.getCoverage ──► org.atgeo.coverageResult
 (bbox → tile URLs)
```

- **Location objects** describe a point, area, address, or grid cell.
- **`org.atgeo.place`** is the place record: it embeds one or more location
  objects and may link to other places via `relations`.
- **Query lexicons** return or resolve places (search, getRecord) or point
  clients at tile files (getCoverage).
- **`listRecords`** is vendored mainly so lexicon schemas themselves can be
  listed from the repo.

Place identity is collection + record key (see `org.atgeo.place`). Gazetteer
URIs look like `https://{host}/{collection}/{rkey}`; AppView-backed places
may use `at://` URIs instead. Cross-place links use `#relation` / `#ref`
rather than `com.atproto.repo.strongRef`, because strongRef requires an
`at://` URI.

---

## Location primitives (`community.lexicon.location.*`)

Shared encodings for a physical location. A place may carry several of these
in its `locations` array (e.g. coordinates plus a street address).

| File | NSID | Role |
|------|------|------|
| [`geo.json`](geo.json) | `community.lexicon.location.geo` | WGS84 point (`latitude`, `longitude` as strings; optional `altitude`, `name`) |
| [`address.json`](address.json) | `community.lexicon.location.address` | Street address (`country` required; region, locality, street, postal code) |
| [`hthree.json`](hthree.json) | `community.lexicon.location.hthree` | [H3](https://h3geo.org/) cell index (`value`) |
| [`bbox.json`](bbox.json) | `community.lexicon.location.bbox` | Axis-aligned bounding box (`north`, `west`, `south`, `east`) |
| [`fsq.json`](fsq.json) | `community.lexicon.location.fsq` | Foursquare Open Source Places POI id (optional coords) |

Coordinates are strings because the AT Protocol data model has no floats.
Values are decimal degrees, WGS84; west/south are negative.

`org.atgeo.place`’s `locations` union currently includes geo, hthree,
address, and bbox. Foursquare ids for gazetteer places are typically carried
in `attributes` rather than as an `fsq` location object.

---

## Place record (`org.atgeo.*`)

| File | NSID | Role |
|------|------|------|
| [`place.json`](place.json) | `org.atgeo.place` | Core place record and related defs |

**`main`** — A geographic place: primary `name`, optional `variants`,
`locations` (union of location primitives above), source-specific
`attributes`, optional `relations` (`within`, `same_as`), and optional
`published_at`.

**`#variant`** — Alternate name (`name`, optional `type` / `language`).

**`#relation`** — Link to a related place (`rkey`, optional `name`), used
under `relations.within` (containing admin regions) and `relations.same_as`
(same entity in another collection).

**`#ref`** — Reference to a place defined elsewhere (`id`, optional `cid`),
with optional denormalized `name` / `locations` / `attributes` for clients
that do not want to resolve the target immediately. Used where a strongRef
cannot apply (e.g. gazetteer `https://` places).

---

## ATGeo query methods

| File | NSID | Role |
|------|------|------|
| [`searchRecords.json`](searchRecords.json) | `org.atgeo.searchRecords` | Search a collection by text (`q`), point (`latitude`/`longitude`), or `bbox` |
| [`getCoverage.json`](getCoverage.json) | `org.atgeo.getCoverage` | Return tile file URLs covering a `bbox` for a collection |
| [`coverageResult.json`](coverageResult.json) | `org.atgeo.coverageResult` | Output shape for getCoverage (`tiles` URI list) |

`searchRecords` results are `#record` objects: `uri`, `attribution`, optional
`cid` / `distance_m`, and `value` typed as `org.atgeo.place`.

---

## Standard repo methods (vendored)

Copies of official AT Protocol lexicons, included so Garganorn can document
and serve the read APIs it implements.

| File | NSID | Role |
|------|------|------|
| [`getRecord.json`](getRecord.json) | `com.atproto.repo.getRecord` | Fetch one record by repo, collection, and rkey |
| [`listRecords.json`](listRecords.json) | `com.atproto.repo.listRecords` | List records in a collection (used for lexicon schema listing) |

---

## Related docs

- Project overview and example queries: [`../../README.md`](../../README.md)
- Published field guide: [atgeo.org/place](https://atgeo.org/place/)
- Upstream location types: [lexicon.community](https://lexicon.community/) (`community.lexicon.location`)
