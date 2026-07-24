<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Auth-o-tron authentication

Configure `authentication` to have auth-o-tron validate incoming credentials
and have Polytope verify the returned JWT locally:

```yaml
authentication:
  url: "https://auth-o-tron.example.com"
  issuer: "https://auth-o-tron.example.com"
  audience: "polytope-server" # optional; this is the default
  public_keys:
    - kid: "key-2026-01"
      public_key: |
        -----BEGIN PUBLIC KEY-----
        ...
        -----END PUBLIC KEY-----
  timeout_ms: 5000
  cache_ttl_secs: 60
  cache_capacity: 10000
  allow_anonymous: false
```

`issuer` and `audience` are exact JWT claim contracts. Polytope accepts only
RS256 tokens with a non-empty `kid`, a configured public key, and matching
`iss` and `aud` claims. RSA keys below 2048 bits, malformed keys, empty
keysets, and duplicate key IDs fail server startup. AuthClient construction is
deliberately fallible so a bad verifier configuration cannot leave the server
running.

The consumer configuration contains public keys only. Keep the auth-o-tron
signing private key in the auth-o-tron deployment; do not mount or copy it
into Polytope.

## Key rotation

`public_keys` is a keyset and may contain overlapping old and new keys. Rotate
without downtime in this order:

1. Add the new public key under its new `kid` while retaining the old key.
2. Deploy Polytope with both public keys.
3. Switch auth-o-tron to sign with the new private key and `kid`.
4. Wait at least the maximum token lifetime.
5. Remove the old public key from Polytope.

The migration from the old shared HMAC `secret` is breaking. The verifier has
no HS256 compatibility mode.
