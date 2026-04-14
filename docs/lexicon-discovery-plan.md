# Lexicon Discovery Plan

Make garganorn's `org.atgeo.*` lexicons discoverable via AT Protocol's standard lexicon resolution mechanism.

## Background

AT Protocol lexicon resolution:

1. DNS TXT lookup: `_lexicon.<authority-domain>` → `did=<DID>`
2. DID resolution: for `did:web`, fetch `https://<hostname>/.well-known/did.json`
3. Extract PDS endpoint from `#atproto_pds` service in DID document
4. Fetch record: `com.atproto.repo.getRecord` with `collection=com.atproto.lexicon.schema`, `rkey=<NSID>`

Group operations (e.g., `goat lex pull org.atgeo.*`) use `com.atproto.repo.listRecords` with `collection=com.atproto.lexicon.schema`, paginated.

No signature verification is performed — resolvers trust the PDS response.

## Current State

Garganorn already:
- Serves lexicon JSON at `/<nsid>` paths
- Handles `com.atproto.repo.getRecord` for place record collections
- Has 8 lexicon files loaded into `gazetteer.lexicons` / `lexicon_map`
- Runs at `places.atgeo.org`

## Work Items

### 1. DNS Configuration (manual)

Add TXT record: `_lexicon.atgeo.org` → `did=did:web:places.atgeo.org`

Covers all `org.atgeo.*` NSIDs.

### 2. DID Document Route

Serve `/.well-known/did.json` from the Flask app.

```json
{
  "id": "did:web:places.atgeo.org",
  "alsoKnownAs": ["at://places.atgeo.org"],
  "verificationMethod": [
    {
      "id": "did:web:places.atgeo.org#atproto",
      "type": "Multikey",
      "controller": "did:web:places.atgeo.org",
      "publicKeyMultibase": "<key>"
    }
  ],
  "service": [
    {
      "id": "#atproto_pds",
      "type": "AtprotoPersonalDataServer",
      "serviceEndpoint": "https://places.atgeo.org"
    }
  ]
}
```

Open question: signing key generation and storage. The key is required for spec compliance even though resolvers currently skip signature verification.

### 3. Extend `getRecord` for Lexicon Schema Collection

When `collection=com.atproto.lexicon.schema`, look up `rkey` in `lexicon_map` and return it as the record value. `getRecord` currently handles only place record collections (DuckDB); it needs a branch for the in-memory lexicon schema collection.

Response format:

```json
{
  "uri": "at://did:web:places.atgeo.org/com.atproto.lexicon.schema/<nsid>",
  "value": { ... }
}
```

### 4. Add `listRecords` for Lexicon Schema Collection

New XRPC method: `com.atproto.repo.listRecords`

Parameters: `collection`, `repo`, `limit` (default 100), `cursor`, `reverse`

For `collection=com.atproto.lexicon.schema`: iterate `lexicon_map` and return paginated results.

Response format:

```json
{
  "records": [
    {
      "uri": "at://did:web:places.atgeo.org/com.atproto.lexicon.schema/<nsid>",
      "cid": "...",
      "value": { ... }
    }
  ],
  "cursor": "..."
}
```

Only needs to support the lexicon schema collection initially.

### 5. Verification

Install `goat`: `brew install goat`

```sh
goat lex resolve org.atgeo.place        # returns lexicon JSON
goat lex pull org.atgeo.*               # downloads all org.atgeo lexicons
goat lex ls org.atgeo.                  # lists all published NSIDs
```

## Notes

- `community.lexicon.location.*` lexicons are served by garganorn but their NSID authority is `community.lexicon`. Publishing those would require a `_lexicon.lexicon.community` DNS TXT record on a domain garganorn doesn't control. Only `org.atgeo.*` lexicons are in scope here.
- Resolvers do not call `com.atproto.server.describeServer` or any other discovery endpoint.
- `goat lex publish` / `goat lex unpublish` use `putRecord` / `deleteRecord`, which garganorn does not need to support (read-only).
