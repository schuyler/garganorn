# Tile-based query: privacy and user safety design

## Threat model

A conventional search endpoint asks the client to send the full query —
search terms, location, filters — to the server. For a gazetteer, this
means the server learns where the user is, where they're going, and what
they're looking for. Repeated queries build a movement profile. Search
terms can reveal intent (shelters, clinics, embassies, protest locations).

If the server is compromised, stolen, or operated by a hostile party,
query logs become a physical safety risk for the user.

## Nothing reveals location without the user opting in

Nothing a client sends may reveal the user's location unless the user took
an action to opt in. The server answers two kinds of search under that rule.

**Tiles.** The client asks which pre-built tiles cover a coarse bounding
box, fetches them, and runs matching, ranking and filtering itself. The
server sees the box and never the query.

**Keyword search.** The server matches a keyword against the names in one
dataset and returns at most 20 places, most notable first. Typing a
keyword is the user's own action, and a keyword is not evidence of where
the user is: people look up places they are not at. A search takes no
point and ranks by no distance, so nothing in it asks "near what?". It may
carry a region, which narrows the results. A region is the thing an app
might attach without asking, a map viewport say, so it is held to whole
degrees; see below.

The keyword still reveals intent, so the server keeps no record of it. An
app that wants the server to learn nothing about what the user is looking
for searches tiles on the client instead.

## A request cannot name a point

Bbox coordinates can leak fine-grained location. A bbox centred on the
user's raw GPS coordinates — e.g. `(-122.41942, 37.77493, ...)` — exposes
their position to ~10 m accuracy regardless of the box's size. A minimum
area check doesn't catch this; the bbox can be large but still precisely
centred on the user.

Tile requests are held to a maximum precision of 0.01° — two decimal places,
a ~1.1 km grid at the equator. This limits the location information the
server can extract to "which grid cell", not "which street corner".

A keyword search region is held to whole degrees: every edge on a 1° grid
line, so the smallest region is one cell about 111 km north to south. The
keyword does the narrowing, so a search has no use for a finer box, and an
attached region says no more than which degree cell the user is looking at.

Both limits are enforced rather than advised: a finer request is refused,
not quietly rounded to the grid, so a client leaking precision is corrected
rather than covered for. Enforcement sits at both ends — clients must snap
before sending, and the server refuses regardless, so a client built
without that logic cannot leak systematically.

Refusal does not protect the coordinates in the refused request. The server
must receive a value to judge its precision, so a request that breaks the
rule has already carried what it broke the rule with. The guarantee is
against sustained exposure rather than against a single request: a leaking
client is caught in its first exchange instead of running unnoticed.

## The keyword is kept in no log

A search keyword travels in the request URL, through a proxy host to the
application host. Neither host writes it to a log:

- **The proxy** deletes the keyword parameter, and the Referer header, from
  every line its logger writes.
- **The application server's access log** records each request's path
  without its query string, and no Referer.
- **The application server's error log** removes the query string from any
  request line it reports, such as a request cut off by a worker timeout.
- **The application** logs no request parameters.

A search response carries no caching header, so no shared cache is invited
to keep the URL either. A seized or compromised server holds no history of
which address searched for what. This covers what the server keeps, not
what it sees: a hostile operator of a running server can record any request
it receives.

## Residual exposure

The bounding box still leaks approximate geographic interest, and a search
region leaks which degree cells interest the user. Attenuating the box
further is left to the client:

- **Client-side tile caching**: reuse previously fetched tiles without
  re-requesting them.
- **Speculative prefetching**: fetch a larger area than immediately needed.

## Design principle

The most trustworthy server is one that never receives the sensitive
information in the first place. Client-side search over tiles eliminates
the query surveillance surface entirely. A keyword search sends the server
what the user chose to type and at most a degree-sized region, and keeps
none of it. The coordinate precision limits keep every spatial
signal coarse enough to protect the user's physical location.
