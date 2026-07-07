# Tile-based query: privacy and user safety design

## Problem

A server-side search endpoint (`searchRecords`) requires the client to send
the full query — search terms, location, filters — to the server. For a
gazetteer, this means the server learns where the user is, where they're
going, and what they're looking for. Repeated queries build a movement
profile. Search terms can reveal intent (shelters, clinics, embassies,
protest locations).

If the server is compromised, stolen, or operated by a hostile party,
query logs become a physical safety risk for the user.

## Solution

The `getCoverage` + tile scheme moves search execution to the client.

1. The client requests tile URLs for a bounding box via `getCoverage`.
2. The server returns pre-built tile file URLs. No search terms are sent.
3. The client downloads tile data and performs matching, ranking, and
   filtering locally.

The server never sees the user's query. It sees only a coarse bounding box,
which is far less revealing than a full search request.

## Transport and caching

The client SDK uses HTTP/2 to fetch tiles. All tile requests for a bbox
multiplex over a single TCP connection, so fetching 18 tiles in dense
Shibuya (~1 MB compressed) costs roughly one round trip plus transfer time,
not 18 sequential fetches.

Tiles are static files with stable URLs — ideal CDN targets. Once any user
in an area triggers a cache fill, subsequent requests hit the CDN edge. This
has both performance and privacy implications: the origin server never sees
the individual tile fetches, only the initial cache misses. The access
pattern that could theoretically reveal user location is absorbed by the CDN
edge, where it's mixed with traffic from all users in the region.

## Residual exposure

The bounding box in the `getCoverage` request still leaks approximate
geographic interest. This can be further attenuated by:

- **Client-side tile caching**: reuse previously fetched tiles without
  re-requesting them.
- **Speculative prefetching**: fetch a larger area than immediately needed.
- **CDN distribution**: as described above, the CDN absorbs tile access
  patterns so the origin server sees only cache misses — a tiny, noisy
  fraction of actual usage.

## Coordinate precision restriction

Even with search terms removed, the bbox coordinates themselves can leak
fine-grained location. A developer who constructs a bbox centered on the
user's raw GPS coordinates — e.g. `(-122.41942, 37.77493, ...)` — exposes
the user's position to ~10 m accuracy regardless of bbox size. A minimum
area check doesn't catch this; the bbox can be large but still precisely
centered on the user.

The server should enforce a **maximum coordinate precision** of 0.01° (two
decimal places), which snaps requests to a ~1.1 km grid at the equator.
This limits the location information the server can extract to "which grid
cell," not "which street corner."

Key design decisions:

- **Reject, don't truncate.** The server returns a `BboxTooPrecise` error
  if any coordinate has more than 2 decimal places. Rejection forces client
  developers to fix their code rather than silently covering for precision
  leaks. A truncation approach would mask bugs and give no signal that the
  client is doing the wrong thing.
- **Both client and server enforce.** The client SDK should snap coordinates
  to the grid before sending. The server-side check is a backstop that
  prevents developers from accidentally bypassing the client-side logic or
  building a client without it. The goal is to make the mistake impossible,
  not merely discouraged.
- **Separate from max-tiles.** The existing `BboxTooLarge` / `max_tiles`
  constraint prevents DoS from world-spanning requests. `BboxTooPrecise`
  protects against a different threat (location leakage) and both checks
  are needed.

## Design principle

The most trustworthy server is one that never receives the sensitive
information in the first place. Phasing out `searchRecords` in favor of
client-side search over tiles eliminates the query surveillance surface
entirely. The coordinate precision restriction ensures that even the
residual bbox signal is coarse enough to protect the user's physical
location.
