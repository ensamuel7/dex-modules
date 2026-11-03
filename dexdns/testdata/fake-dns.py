#!/usr/bin/env python3
"""A stand-in for Cloudflare's DNS API and Hetzner's (Cloud API zones and
RRsets), for testing DexRail's DNS without an account at either.

    scripts/fake-dns.py [port]        default 8898

Cloudflare is under /client/v4 (CLOUDFLARE_API=http://127.0.0.1:8898/client/v4),
Hetzner under /v1 (HCLOUD_API=http://127.0.0.1:8898/v1). Tokens are checked:
"cf-good" and "hz-good" work, anything else is refused. One zone exists in each
from the start, cf.example and hz.example; others are made when asked for.
GET /state answers with every record, for tests to compare against.
"""
import json
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

lock = threading.Lock()
cf_zones = {"z-cf": {"id": "z-cf", "name": "cf.example", "status": "active",
                     "name_servers": ["ada.ns.cloudflare.com", "bob.ns.cloudflare.com"]}}
cf_records = {}  # id -> record
hz_zones = {1: {"id": 1, "name": "hz.example", "mode": "primary", "ttl": 3600,
                "authoritative_nameservers": {"assigned": ["hydrogen.ns.hetzner.com", "oxygen.ns.hetzner.com",
                                                           "helium.ns.hetzner.de"],
                                              "delegation_status": "valid"}}}
hz_rrsets = {}  # (zone, name, type) -> rrset
next_zone = [2]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def send(self, status, body):
        data = json.dumps(body).encode() if body is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def token(self):
        return self.headers.get("Authorization", "").replace("Bearer ", "")

    def handle_any(self):
        u = urlparse(self.path)
        if u.path == "/state":
            with lock:
                cf = sorted("%s %s %s" % (r["name"], r.get("type", "A"), r.get("content", json.dumps(r.get("data"))))
                            for r in cf_records.values())
                hz = sorted("%s/%s %s %s" % (hz_zones[k[0]]["name"], k[1], k[2], v["value"])
                            for k, s in hz_rrsets.items() for v in s["records"])
            return self.send(200, {"cloudflare": cf, "hetzner": hz})
        if u.path.startswith("/client/v4/"):
            return self.cloudflare(u)
        if u.path.startswith("/v1/"):
            return self.hetzner(u)
        self.send(404, {})

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = handle_any

    # Cloudflare: {success, errors, result}.
    def cf(self, status, result, message=""):
        errors = [{"code": 1000, "message": message}] if message else []
        self.send(status, {"success": not message, "errors": errors, "messages": [], "result": result})

    def cloudflare(self, u):
        if self.token() != "cf-good":
            return self.cf(403, None, "Authentication error")
        parts = u.path.split("/")[3:]
        q = parse_qs(u.query)
        with lock:
            if parts == ["zones"] and self.command == "GET":
                name = q.get("name", [""])[0]
                return self.cf(200, [z for z in cf_zones.values() if z["name"] == name])
            if parts == ["accounts"]:
                return self.cf(200, [{"id": "acct-1", "name": "Fake"}])
            if parts == ["zones"] and self.command == "POST":
                b = self.body()
                zid = "z-" + uuid.uuid4().hex[:6]
                cf_zones[zid] = {"id": zid, "name": b["name"], "status": "pending",
                                 "name_servers": ["cat.ns.cloudflare.com", "dan.ns.cloudflare.com"]}
                return self.cf(200, cf_zones[zid])
            if len(parts) >= 3 and parts[0] == "zones" and parts[2] == "dns_records":
                zid = parts[1]
                if zid not in cf_zones:
                    return self.cf(404, None, "Invalid zone identifier")
                if self.command == "GET":
                    kind = q.get("type", [""])[0]
                    name = q.get("name", [None])[0]
                    found = [r for r in cf_records.values()
                             if r["zone_id"] == zid and (not kind or r["type"] == kind)
                             and (name is None or r["name"] == name)]
                    per = int(q.get("per_page", ["100"])[0])
                    page = int(q.get("page", ["1"])[0])
                    pages = max(1, (len(found) + per - 1) // per)
                    errors = []
                    self.send(200, {"success": True, "errors": errors, "messages": [],
                                    "result": found[(page - 1) * per:page * per],
                                    "result_info": {"page": page, "per_page": per, "total_pages": pages,
                                                    "total_count": len(found)}})
                    return
                if self.command == "POST":
                    b = self.body()
                    clash = [r for r in cf_records.values() if r["name"] == b["name"] and r["type"] == "CNAME"]
                    if clash:
                        return self.cf(400, None, "An A, AAAA, or CNAME record with that host already exists.")
                    rid = uuid.uuid4().hex[:8]
                    cf_records[rid] = dict(b, id=rid, zone_id=zid)
                    return self.cf(200, cf_records[rid])
                if self.command == "DELETE" and len(parts) == 4:
                    if cf_records.pop(parts[3], None) is None:
                        return self.cf(404, None, "Record not found")
                    return self.cf(200, {"id": parts[3]})
        self.cf(404, None, "fake-dns does not implement this")

    # Hetzner: errors as {error: {code, message}}.
    def hz(self, status, message):
        self.send(status, {"error": {"code": "invalid_input", "message": message}})

    def hetzner(self, u):
        if self.token() != "hz-good":
            return self.send(401, {"error": {"code": "unauthorized", "message": "unable to authenticate"}})
        parts = [unquote(p) for p in u.path.split("/")[2:]]
        q = parse_qs(u.query)
        with lock:
            if parts == ["zones"] and self.command == "GET":
                name = q.get("name", [""])[0]
                return self.send(200, {"zones": [z for z in hz_zones.values() if z["name"] == name]})
            if parts == ["zones"] and self.command == "POST":
                b = self.body()
                zid = next_zone[0]
                next_zone[0] += 1
                hz_zones[zid] = {"id": zid, "name": b["name"], "mode": b.get("mode", "primary"), "ttl": 3600,
                                 "authoritative_nameservers": {"assigned": ["hydrogen.ns.hetzner.com"],
                                                               "delegation_status": "unregistered"}}
                return self.send(201, {"zone": hz_zones[zid], "action": {"id": 1, "status": "success"}})
            if len(parts) >= 3 and parts[0] == "zones" and parts[2] == "rrsets":
                zid = int(parts[1])
                if zid not in hz_zones:
                    return self.hz(404, "zone not found")
                if len(parts) == 3 and self.command == "GET":
                    name = q.get("name", [None])[0]
                    kind = q.get("type", [None])[0]
                    found = [v for k, v in sorted(hz_rrsets.items(), key=lambda kv: (kv[0][1], kv[0][2]))
                             if k[0] == zid and (name is None or k[1] == name) and (kind is None or k[2] == kind)]
                    per = int(q.get("per_page", ["25"])[0])
                    page = int(q.get("page", ["1"])[0])
                    last = max(1, (len(found) + per - 1) // per)
                    return self.send(200, {"rrsets": found[(page - 1) * per:page * per],
                                           "meta": {"pagination": {"page": page, "per_page": per,
                                                                   "next_page": page + 1 if page < last else None,
                                                                   "last_page": last}}})
                if len(parts) == 3 and self.command == "POST":
                    b = self.body()
                    key = (zid, b["name"], b["type"])
                    if key in hz_rrsets:
                        return self.hz(409, "rrset already exists")
                    hz_rrsets[key] = {"name": b["name"], "type": b["type"], "ttl": b.get("ttl"),
                                      "records": b.get("records", [])}
                    return self.send(201, {"rrset": hz_rrsets[key], "action": {"id": 1, "status": "success"}})
                if len(parts) >= 5:
                    key = (zid, parts[3], parts[4])
                    if len(parts) == 5 and self.command == "GET":
                        if key not in hz_rrsets:
                            return self.send(404, {"error": {"code": "not_found", "message": "rrset not found"}})
                        return self.send(200, {"rrset": hz_rrsets[key]})
                    if len(parts) == 5 and self.command == "DELETE":
                        if hz_rrsets.pop(key, None) is None:
                            return self.send(404, {"error": {"code": "not_found", "message": "rrset not found"}})
                        return self.send(201, {"action": {"id": 1, "status": "success"}})
                    if parts[5:] == ["actions", "set_records"]:
                        if key not in hz_rrsets:
                            return self.send(404, {"error": {"code": "not_found", "message": "rrset not found"}})
                        hz_rrsets[key]["records"] = self.body()["records"]
                        return self.send(201, {"action": {"id": 1, "status": "success"}})
        self.hz(404, "fake-dns does not implement this")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8898
    print("fake-dns on :%d" % port, flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
