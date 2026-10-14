# Publishing the org.atgeo lexicons

The `org.atgeo.*` lexicons in `garganorn/lexicon/` are published as schema
records in the repo of a publishing DID, and a DNS TXT record names that DID
as the authority for the `org.atgeo` namespace. The
`community.lexicon.location.*` files in the same directory are excluded:
another authority publishes them.

## DNS

NSID authority lookup for `org.atgeo.*` reads the TXT record on
`_lexicon.atgeo.org`:

```
_lexicon.atgeo.org.  TXT  "did=<DID>"
```

`<DID>` is the DID of the account that publishes. If the record names garganorn's
service DID (`did:web:places.atgeo.org`), point it at the publishing account
first: the service DID advertises no PDS.

## Check

The `org.atgeo.*` files, from the repository root (bash or zsh):

```
L=garganorn/lexicon
FILES=($L/place.json $L/defs.json $L/tilePayload.json $L/getTiles.json
  $L/getPlace.json $L/describeTileset.json $L/describeService.json)

goat lex lint "${FILES[@]}"
goat lex parse "${FILES[@]}"
goat lex check-dns --lexicons-dir $L "${FILES[@]}"
dig +short TXT _lexicon.atgeo.org
```

`check-dns` reports only NSID groups with no TXT record at all; it cannot
tell which DID a record names. `dig` shows that, and it must be `<DID>`.

## Publish

```
goat lex publish --lexicons-dir $L --username <DID> "${FILES[@]}"
```

Authenticate with an app password (`--password` or `GOAT_PASSWORD`). By
default `publish` writes only lexicons whose DNS matches the authenticated
account and skips schemas identical to the published ones. `--update`
replaces changed ones; run `goat lex breaking --lexicons-dir $L "${FILES[@]}"`
first, since a published lexicon is hard to change compatibly.
`goat lex status --lexicons-dir $L "${FILES[@]}"` compares the local files with the
network.
