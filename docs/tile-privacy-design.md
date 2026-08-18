# Tile-based query: privacy and user safety design

## Threat model

A server-side search endpoint requires the client to send
the full query — search terms, location, filters — to the server. For a
gazetteer, this means the server learns where the user is, where they're
going, and what they're looking for. Repeated queries build a movement
profile. Search terms can reveal intent (shelters, clinics, embassies,
protest locations).

If the server is compromised, stolen, or operated by a hostile party,
query logs become a physical safety risk for the user.

## Search must execute on the client

The server offers no endpoint that accepts search terms. It serves
pre-built tiles and the URLs that locate them; matching, ranking and
filtering must all run on the client, over tile data it has already
fetched.

The server therefore never sees the user's query. It sees only a coarse
bounding box, which is far less revealing than a full search request.

## A request cannot name a point

Even with search terms removed, bbox coordinates themselves can leak
fine-grained location. A bbox centred on the user's raw GPS coordinates —
e.g. `(-122.41942, 37.77493, ...)` — exposes their position to ~10 m
accuracy regardless of the box's size. A minimum area check doesn't catch
this; the bbox can be large but still precisely centred on the user.

Coordinates are held to a maximum precision of 0.01° — two decimal places,
a ~1.1 km grid at the equator. This limits the location information the
server can extract to "which grid cell", not "which street corner".

The limit is enforced rather than advised: a finer request is refused, not
quietly rounded to the grid, so a client leaking precision is corrected
rather than covered for. Enforcement sits at both ends — clients must snap
before sending, and the server refuses regardless, so a client built
without that logic cannot leak systematically.

Refusal does not protect the coordinates in the refused request. The server
must receive a value to judge its precision, so a request that breaks the
rule has already carried what it broke the rule with. The guarantee is
against sustained exposure rather than against a single request: a leaking
client is caught in its first exchange instead of running unnoticed.

## Residual exposure

The bounding box still leaks approximate geographic interest. Attenuating
it further is left to the client:

- **Client-side tile caching**: reuse previously fetched tiles without
  re-requesting them.
- **Speculative prefetching**: fetch a larger area than immediately needed.

## Design principle

The most trustworthy server is one that never receives the sensitive
information in the first place. Client-side search over tiles eliminates
the query surveillance surface entirely. The coordinate precision limit
keeps even the residual bbox signal coarse enough to protect the user's
physical location.
