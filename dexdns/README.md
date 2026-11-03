# dexdns

A domain's DNS records at Cloudflare, Hetzner (Cloud API) or a PowerDNS
Authoritative server, behind one interface. Pure Dex: `http` and `json` only.

```dex
import "github.com/ensamuel7/dex-modules/dexdns"

let p: dexdns.Cloudflare = dexdns.cloudflare("https://api.cloudflare.com/client/v4", token)
p.comment = "Kept by my-app"
let z: dexdns.Zone = p.zone("example.com")

// Exactly these MX records.
p.replace(z, "example.com", "example.com", "MX", [
    dexdns.Record { name: "example.com", type: "MX", content: "mail.example.com", ttl: 3600, priority: 10 }
])

// Our SPF in, the old one out; a user's other TXT records at the apex stay.
p.update(z, "example.com", "example.com", "TXT", [newSpf], [oldSpf])
```

| Provider | Constructor |
|---|---|
| Cloudflare | `cloudflare(api, token)`; set `comment` to tag records made |
| Hetzner | `hetzner(api, token)`; `comment` likewise |
| PowerDNS | `powerdns(api, key, nameservers, label)`; `label` names it in problems |

Each implements `Provider`: `kind`, `zone` (found, or made), `list`,
`replace`, `update` and `forget`. Writes return `""` when done, or the
provider's reason.

**Records are provider-neutral.** Names and hostnames are full, lowercase and
without the trailing dot; TXT is the whole unquoted text, split into 255-byte
strings on the wire where the provider needs it and joined back on reading;
MX and SRV keep their priority in `priority`, SRV content is
`weight port target`. `sameValue` compares two records as a provider would.

**`update` touches only what it is given.** It drops the `drop` values, adds
the `add` values, and leaves every other value at that name and type alone.
A program that keeps a note of the records it made can edit those and nothing
else -- the way two programs share a zone. Neither `replace` nor `update`
compares TTL or proxying: an existing record with the same value is kept.

## Tests

```sh
./test.sh              # unit tests, then Cloudflare and Hetzner against
                       # testdata/fake-dns.py and PowerDNS 4.9 in Docker
NO_DOCKER=1 ./test.sh  # without PowerDNS
```
