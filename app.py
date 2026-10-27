"""Options Flow Board - mirror for Render (free plan). View-only.

The trading engine stays on the engine box. A pusher on the box (render_pusher.py) GETs the board's read-only JSON
endpoints from the engine every few seconds and POSTs them here (/_push, bearer token). This service only serves those
snapshots back, behind the same view-only login as view_proxy.py (user "abhinav" + SPCX_VIEW_PASSWORD, optional
"guest" + SPCX_GUEST_PASSWORD while guest access is switched on on the box). No write/trade endpoint exists here.

Env (all secrets set in the Render dashboard / API, never in git):
  SPCX_VIEW_PASSWORD, SPCX_GUEST_PASSWORD (optional), SPCX_PUSH_TOKEN, SPCX_SESSION_KEY,
  SPCX_KV_URL (optional, redis://host:port of a Render Key Value: keeps the last snapshot across spin-down/restarts),
  SPCX_KEEPALIVE = session (default: self-ping during the US session so a viewer never waits for a cold start) | always | off
Stdlib only."""
import base64, gzip, hashlib, hmac, html, json, os, re, secrets, socket, sys, threading, time, urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("PORT") or 10000)
USER, GUEST = "abhinav", "guest"
_PW = os.environ.get("SPCX_VIEW_PASSWORD", "").strip()
_GPW = os.environ.get("SPCX_GUEST_PASSWORD", "").strip()
_TOK = os.environ.get("SPCX_PUSH_TOKEN", "").strip().encode()
_SKEY = (os.environ.get("SPCX_SESSION_KEY", "").strip() or secrets.token_urlsafe(32)).encode()
if not _PW or len(_TOK) < 24:
    sys.stderr.write("SPCX_VIEW_PASSWORD / SPCX_PUSH_TOKEN not set - refusing to start\n"); sys.exit(2)
USERS = {USER: _PW.encode()}
if _GPW: USERS[GUEST] = _GPW.encode()
del _PW, _GPW
KV_URL = os.environ.get("SPCX_KV_URL", "").strip()
KEEPALIVE = (os.environ.get("SPCX_KEEPALIVE") or "session").strip().lower()
SELF_URL = (os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

COOKIE, TTL, GUEST_TTL = "spcx_session", 7 * 24 * 3600, 24 * 3600
MAX_FAILS, WINDOW = 5, 600
PATHS = {"/api/board", "/api/state", "/api/minutes", "/api/version", "/api/config"}
CORE = ("/api/board", "/api/version", "/api/config", "/api/state?ticker=SPCX", "/api/minutes?ticker=SPCX")
DATE_RE, TK_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$"), re.compile(r"^[A-Z0-9.\-]{1,12}$")
STALE_S = 30

def norm_key(path, query):
    """Canonical snapshot key, identical on the pusher side. None if not an allowed read-only GET."""
    if path not in PATHS: return None
    q = {k: v[0] for k, v in parse_qs(query or "").items() if v}
    out = {}
    if path in ("/api/board", "/api/state", "/api/minutes") and q.get("date"):
        if not DATE_RE.match(q["date"]): return None
        out["date"] = q["date"]
    if path in ("/api/state", "/api/minutes") and q.get("ticker"):
        t = q["ticker"].strip().upper()
        if not TK_RE.match(t): return None
        out["ticker"] = t
    if path in ("/api/state", "/api/minutes") and "ticker" not in out: out["ticker"] = "SPCX"
    return path + ("?" + urlencode(sorted(out.items())) if out else "")

# ---------------------------------------------------------------- snapshot store
LOCK = threading.Lock(); COND = threading.Condition(LOCK)
SNAP = {}          # key -> {"gz": bytes, "ct": str, "ver": int, "at": float (render clock, received)}
META = {"version": None, "hb": 0.0, "links": {}, "guest_gen": None, "index_gz": None, "index_sha": None,
        "box_time": None, "pushes": 0, "started": time.time(), "restored": False}
WANT = {}          # key -> last time a viewer asked for it
FAILS = {}; GFAILS = []

def key_age(k, now=None):
    now = now or time.time(); s = SNAP.get(k)
    if not s: return None
    cur = s["at"]
    if META["version"] is not None and s["ver"] == META["version"]: cur = max(cur, META["hb"])
    return max(0.0, now - cur)

# ---------------------------------------------------------------- tiny RESP (Redis/Valkey) client for persistence
def _kv(*args):
    u = urlparse(KV_URL); s = socket.create_connection((u.hostname, u.port or 6379), timeout=8)
    try:
        if u.password:
            a = [b"AUTH"] + ([u.username.encode()] if u.username else []) + [u.password.encode()]
            s.sendall(_enc(a)); _read(s.makefile("rb"))
        s.sendall(_enc([a if isinstance(a, bytes) else str(a).encode() for a in args]))
        return _read(s.makefile("rb"))
    finally:
        s.close()
def _enc(parts): return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)
def _read(f):
    line = f.readline(); t, rest = line[:1], line[1:-2]
    if t == b"$":
        n = int(rest); return None if n < 0 else f.read(n + 2)[:-2]
    if t in (b"+", b":"): return rest
    if t == b"-": raise RuntimeError(rest.decode(errors="replace"))
    if t == b"*": return [_read(f) for _ in range(max(0, int(rest)))]
    raise RuntimeError("bad reply")

def persist():
    if not KV_URL: return
    with LOCK:
        snaps = {k: SNAP[k] for k in CORE if k in SNAP}
        meta = {k: META[k] for k in ("version", "hb", "links", "guest_gen", "index_sha", "box_time")}
        idx = META["index_gz"]
    blob = {"meta": meta, "snaps": {k: {"gz": base64.b64encode(v["gz"]).decode(), "ct": v["ct"], "ver": v["ver"], "at": v["at"]} for k, v in snaps.items()},
            "index_gz": base64.b64encode(idx).decode() if idx else None}
    _kv("SET", "spcx:mirror:v1", gzip.compress(json.dumps(blob).encode(), 3))

def restore():
    if not KV_URL: return
    try:
        raw = _kv("GET", "spcx:mirror:v1")
        if not raw: return
        b = json.loads(gzip.decompress(raw))
        with LOCK:
            for k, v in b["snaps"].items():
                SNAP[k] = {"gz": base64.b64decode(v["gz"]), "ct": v["ct"], "ver": v["ver"], "at": v["at"]}
            META.update(b["meta"]); META["restored"] = True
            if b.get("index_gz"): META["index_gz"] = base64.b64decode(b["index_gz"])
        print(f"restored {len(b['snaps'])} snapshots from Key Value", flush=True)
    except Exception as e:
        print("KV restore failed:", repr(e)[:200], flush=True)

def persist_loop():
    last = 0
    while True:
        time.sleep(5)
        if META["pushes"] and META["hb"] > last and time.time() - last >= 20:
            try: persist(); last = META["hb"]
            except Exception as e: print("KV persist failed:", repr(e)[:200], flush=True)

def keepalive_loop():
    """Free instances spin down after 15 min without inbound traffic. While the box pushes, that never happens.
    When the box sleeps, ping our own public URL during the US session (Mon-Fri 9:00-16:30 ET) so a viewer gets an
    instant page with the last snapshot. ~7.5 h/day of free instance hours; set SPCX_KEEPALIVE=off to disable."""
    if KEEPALIVE == "off" or not SELF_URL: return
    while True:
        time.sleep(600)
        try:
            et = datetime.now(timezone.utc) + timedelta(hours=-4 if _us_dst() else -5)
            in_sess = et.weekday() < 5 and (9 * 60) <= et.hour * 60 + et.minute <= (16 * 60 + 30)
            if KEEPALIVE == "always" or in_sess:
                urllib.request.urlopen(SELF_URL + "/_alive", timeout=20).read()
        except Exception:
            pass
def _us_dst():
    n = datetime.now(timezone.utc); y = n.year
    mar = datetime(y, 3, 8, 7, tzinfo=timezone.utc); mar += timedelta(days=(6 - mar.weekday()) % 7)     # 2nd Sun Mar 2am ET
    nov = datetime(y, 11, 1, 6, tzinfo=timezone.utc); nov += timedelta(days=(6 - nov.weekday()) % 7)    # 1st Sun Nov 2am ET
    return mar <= n < nov

# ---------------------------------------------------------------- auth (same rules as view_proxy.py; stateless signed cookie)
def check(user, pw):
    u = user.strip().lower().encode(); p = pw.strip().encode(); hit = None
    for name in (USER, GUEST):
        pw_b = USERS.get(name, secrets.token_bytes(24))
        if hmac.compare_digest(u, name.encode()) & hmac.compare_digest(p, pw_b) and name in USERS: hit = name
    if hit == GUEST and not META["guest_gen"]: return None, "guest access is switched off"
    return (hit, "") if hit else (None, "mismatch")
def _sign(s): return base64.urlsafe_b64encode(hmac.new(_SKEY, s.encode(), hashlib.sha256).digest()[:24]).decode().rstrip("=")
def make_cookie(user):
    exp = int(time.time() + (GUEST_TTL if user == GUEST else TTL)); gen = (META["guest_gen"] or "-") if user == GUEST else "-"
    body = f"{user}.{exp}.{gen}"; return f"{body}.{_sign(body)}", exp - int(time.time())
def read_cookie(v):
    try:
        user, exp, gen, sig = v.split(".")
    except ValueError:
        return None
    if not hmac.compare_digest(sig, _sign(f"{user}.{exp}.{gen}")) or int(exp) < time.time() or user not in USERS: return None
    if user == GUEST and (not META["guest_gen"] or not hmac.compare_digest(gen, META["guest_gen"])): return None
    return user

LOGIN_HTML = """<!doctype html><html lang=en><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Options-Flow Engine - sign in</title><style>
body{margin:0;background:#0f1218;color:#e6e9ef;font:16px/1.4 -apple-system,system-ui,sans-serif;display:flex;min-height:100vh;align-items:center;justify-content:center}
form{background:#171b23;border:1px solid #2a303c;border-radius:12px;padding:24px;width:min(340px,90vw)}
h1{font-size:18px;margin:0 0 4px}p{color:#8b93a3;font-size:13px;margin:0 0 16px}label{display:block;font-size:13px;color:#8b93a3;margin:12px 0 4px}
input{width:100%;box-sizing:border-box;font-size:16px;padding:10px;border-radius:8px;border:1px solid #2a303c;background:#0c0f14;color:#e6e9ef}
button{width:100%;margin-top:18px;padding:12px;font-size:16px;font-weight:600;border:0;border-radius:8px;background:#5b9cff;color:#000}
.err{color:#e5484d;font-size:14px;margin-top:12px}.links{font-size:12px;color:#8b93a3;margin-top:16px;word-break:break-all}.links a{color:#5b9cff}
.st{font-size:12px;color:#8b93a3;margin-top:14px}.st i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;background:%DOT%}</style></head><body>
<form method=post action=/login autocomplete=on><h1>Options-Flow Engine</h1><p>View-only access. Always-on mirror of the live board.</p>
<label for=u>Username</label><input id=u name=username value="%USER%" autocomplete=username autocapitalize=off autocorrect=off spellcheck=false>
<label for=p>Password</label><input id=p name=password type=password autocomplete=current-password autocapitalize=off autocorrect=off spellcheck=false autofocus>
<button type=submit>Sign in</button>%ERR%<div class=st><i></i>%STATUS%</div>%LINKS%</form></body></html>"""

def status_txt():
    a = key_age("/api/board")
    if a is None: return "#8b93a3", "Waiting for the first snapshot from the engine box."
    if a <= STALE_S: return "#2fbf71", "Engine feed live."
    return "#f0a73a", f"Engine box offline - last snapshot {_ago(a)} ago."
def _ago(a):
    a = int(a); return f"{a}s" if a < 90 else f"{a // 60}m" if a < 5400 else f"{a // 3600}h {a % 3600 // 60}m"

# ---------------------------------------------------------------- index.html with mirror freshness indicator
INJECT = r"""<script>
/* render mirror: freshness comes from the snapshot age the mirror reports, not from when this browser fetched it */
(function(){const of=window.fetch;window.__snapAt=0;window.__boxHb=0;
window.fetch=async function(u,o){const r=await of(u,o);try{const p=String(u);const a=r.headers.get('X-SPCX-Snap-Age');
 if(a!==null&&p.indexOf('/api/board')===0)window.__snapAt=Date.now()-parseFloat(a)*1000;
 const h=r.headers.get('X-SPCX-Box-Age');if(h!==null)window.__boxHb=Date.now()-parseFloat(h)*1000;}catch(e){}return r};
const ist=t=>new Date(t).toLocaleTimeString('en-US',{timeZone:'Asia/Kolkata',hour:'numeric',minute:'2-digit',second:'2-digit'});
const ago=a=>a<60?a+'s':a<3600?Math.floor(a/60)+'m '+(a%60)+'s':Math.floor(a/3600)+'h '+Math.floor(a%3600/60)+'m';
window.__liveTick2=function(B_){const el=document.getElementById('live'),tx=document.getElementById('livetx');if(!el||!tx)return true;
 let bar=document.getElementById('mirbar');if(!bar){bar=document.createElement('div');bar.id='mirbar';
  bar.style.cssText='display:none;margin:10px 16px 0;padding:9px 12px;border:1px solid #6b4e12;background:#2a2112;color:#f0c36a;border-radius:8px;font:600 13px/1.4 system-ui,Segoe UI,Roboto,sans-serif';
  const hd=document.querySelector('header');hd&&hd.parentNode?hd.parentNode.insertBefore(bar,hd.nextSibling):document.body.prepend(bar)}
 const bd=[...document.querySelectorAll('header .paper')].find(e=>/LIVE DATA|STALE/.test(e.textContent));
 if(!window.__snapAt){tx.textContent='Connecting…';return true}
 const a=Math.max(0,Math.round((Date.now()-window.__snapAt)/1000));
 el.className='lv'+(a>120?' rd':a>STALE?' am':'');
 const lab=a>STALE?'Stale':(B_&&B_.live_day===false?'Showing '+(B_.date||'past day'):B_&&!B_.session_open?'Market closed':'Live');
 tx.textContent=lab+' · updated '+ago(a)+' ago';tx.title='Snapshot from the engine box at '+ist(window.__snapAt)+' IST (always-on mirror)';
 if(a>STALE){bar.innerHTML='';const b=document.createElement('b');b.textContent='Stale snapshot';bar.appendChild(b);
  bar.appendChild(document.createTextNode(' · last updated '+ist(window.__snapAt)+' IST ('+ago(a)+' ago). The engine box is asleep or its feed is paused; this page keeps checking and goes live again by itself.'));bar.style.display='block'}
 else bar.style.display='none';
 if(bd){if(!bd.dataset.st0)bd.dataset.st0=bd.getAttribute('style')||'';if(a>STALE){bd.textContent='STALE';bd.setAttribute('style','background:#3d2e0f;color:#f0c36a;border-color:#7a5a1f')}else{bd.textContent='LIVE DATA';bd.setAttribute('style',bd.dataset.st0)}}
 return true};
})();
</script>""".replace("STALE", str(STALE_S))
def render_index():
    gz = META["index_gz"]
    raw = gzip.decompress(gz).decode() if gz else open(os.path.join(HERE, "static", "index.html"), encoding="utf-8").read()
    raw = raw.replace("function liveTick(){", "function liveTick(){if(window.__liveTick2&&window.__liveTick2(typeof B!=='undefined'?B:null))return;", 1)
    i = raw.find("<script"); raw = raw[:i] + INJECT + raw[i:] if i >= 0 else raw + INJECT
    return raw.encode()

# ---------------------------------------------------------------- HTTP
class H(BaseHTTPRequestHandler):
    server_version = "options-flow-board"; sys_version = ""; protocol_version = "HTTP/1.1"
    def log_message(self, fmt, *a):
        p = urlparse(self.path).path
        if p not in ("/_push", "/_alive") and not p.startswith("/api/"): sys.stderr.write("%s %s\n" % (self.command, fmt % a))
    def _ip(self):
        c = self.headers.get("CF-Connecting-IP") or self.headers.get("True-Client-IP")
        if c: return c.strip()
        x = self.headers.get("X-Forwarded-For"); return x.split(",")[0].strip() if x else self.client_address[0]
    def _send(self, code, body=b"", ctype="text/plain; charset=utf-8", extra=(), gz=False):
        if isinstance(body, str): body = body.encode()
        if gz and "gzip" not in (self.headers.get("Accept-Encoding") or ""): body = gzip.decompress(body); gz = False
        elif not gz and len(body) > 1024 and "gzip" in (self.headers.get("Accept-Encoding") or "") and ctype.split(";")[0] in ("text/html", "application/json"):
            body = gzip.compress(body, 5); gz = True
        self.send_response(code)
        for k, v in extra: self.send_header(k, v)
        if gz: self.send_header("Content-Encoding", "gzip"); self.send_header("Vary", "Accept-Encoding")
        self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store"); self.send_header("X-SPCX-View-Only", "1")
        self.send_header("X-Frame-Options", "DENY"); self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        if META["hb"]: self.send_header("X-SPCX-Box-Age", "%.1f" % max(0, time.time() - META["hb"]))
        self.end_headers()
        if self.command != "HEAD": self.wfile.write(body)
    def _cookie(self):
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == COOKIE and v: return v
    def _authed(self):
        got = self.headers.get("Authorization") or ""
        if got[:6].lower() == "basic ":
            try: u, _, pw = base64.b64decode(got[6:].strip()).decode("utf-8", "replace").partition(":")
            except Exception: u, pw = "", ""
            who, _ = check(u, pw)
            if who: return who
        c = self._cookie(); return read_cookie(c) if c else None
    def _unauth(self, p):
        if p.startswith("/api/") or p == "/healthz":
            return self._send(401, json.dumps({"error": "authentication required", "login": "/login"}), "application/json")
        return self._send(302, "", extra=[("Location", "/login")])
    def _login_page(self, code=200, err="", user=None):
        if user is None:
            user = GUEST if (parse_qs(urlparse(self.path).query).get("u") or [""])[0].strip().lower() == GUEST else USER
        L = META["links"] or {}
        rows = [f'{lab}: <a href="{html.escape(u)}/login">{html.escape(u)}</a>' for lab, u in (("Main link", L.get("primary")), ("Backup link", L.get("backup"))) if u]
        lk = f'<div class=links>Direct links to the engine box (only work while it is awake):<br>{"<br>".join(rows)}</div>' if rows else ""
        dot, st = status_txt()
        e = f'<div class=err>{html.escape(err)}</div>' if err else ""
        return self._send(code, LOGIN_HTML.replace("%ERR%", e).replace("%LINKS%", lk).replace("%USER%", html.escape(user))
                          .replace("%DOT%", dot).replace("%STATUS%", html.escape(st)), "text/html; charset=utf-8")

    def do_POST(self):
        p = urlparse(self.path).path
        if p == "/_push": return self._push()
        if p != "/login":
            return self._send(403, "view-only: this action is disabled on the public link") if self._authed() else self._unauth(p)
        ip, now = self._ip(), time.time()
        with LOCK:
            FAILS[ip] = [t for t in FAILS.get(ip, []) if now - t < WINDOW]
            GFAILS[:] = [t for t in GFAILS if now - t < WINDOW]
            if len(FAILS[ip]) >= MAX_FAILS or len(GFAILS) >= 100: return self._login_page(429, "Too many failed attempts. Try again in 10 minutes.")
        n = min(int(self.headers.get("Content-Length") or 0), 4096)
        form = parse_qs(self.rfile.read(n).decode(errors="replace"))
        uname = (form.get("username") or [""])[0]
        who, why = check(uname, (form.get("password") or [""])[0])
        if not who:
            sys.stderr.write(f"AUTH-FAIL form: {why} ip={ip}\n")
            with LOCK: FAILS[ip].append(now); GFAILS.append(now)
            return self._login_page(401, "Wrong username or password.", user=uname.strip().lower() or USER)
        if who == GUEST: sys.stderr.write(f"GUEST-LOGIN ip={ip}\n")
        with LOCK: FAILS.pop(ip, None)
        v, ttl = make_cookie(who)
        return self._send(303, "", extra=[("Location", "/"), ("Set-Cookie", f"{COOKIE}={v}; Max-Age={ttl}; Path=/; HttpOnly; Secure; SameSite=Lax")])

    def _push(self):
        tok = (self.headers.get("Authorization") or "")[7:].strip().encode()
        if not hmac.compare_digest(tok, _TOK):
            sys.stderr.write(f"PUSH-REJECTED ip={self._ip()}\n"); return self._send(403, "forbidden")
        n = int(self.headers.get("Content-Length") or 0)
        if n > 16 * 1024 * 1024: return self._send(413, "too large")
        raw = self.rfile.read(n)
        try:
            hdr_len = int(raw[:10]); hdr = json.loads(raw[10:10 + hdr_len]); off = 10 + hdr_len
            now = time.time(); items = []
            for it in hdr.get("items", []):
                blob = raw[off:off + it["len"]]; off += it["len"]
                k = it["key"]; pu = urlparse(k)
                if k != "__index__" and norm_key(pu.path, pu.query) != k: continue
                gzip.decompress(blob)[:1]            # must be valid gzip
                items.append((k, blob, it.get("ct") or "application/json", int(it.get("ver") or 0)))
        except Exception as e:
            return self._send(400, "bad push: " + repr(e)[:100])
        with COND:
            for k, blob, ct, ver in items:
                if k == "__index__":
                    META["index_gz"] = blob; META["index_sha"] = hashlib.sha256(blob).hexdigest()
                else:
                    SNAP[k] = {"gz": blob, "ct": ct, "ver": ver, "at": now}
            if hdr.get("version") is not None: META["version"] = int(hdr["version"])
            META["hb"] = now; META["pushes"] += 1; META["box_time"] = hdr.get("box_time")
            if isinstance(hdr.get("links"), dict):
                META["links"] = {k: v for k, v in hdr["links"].items() if k in ("primary", "backup") and isinstance(v, str) and v.startswith("https://") and len(v) < 200}
            g = hdr.get("guest_gen"); META["guest_gen"] = g if isinstance(g, str) and re.match(r"^[0-9a-f]{16,64}$", g) else None
            for k in [k for k, t in WANT.items() if now - t > 120]: WANT.pop(k, None)
            want = sorted(WANT, key=lambda k: -WANT[k])[:12]
            have = {k: SNAP[k]["ver"] for k in set(want) | set(CORE) if k in SNAP}
            COND.notify_all()
        return self._send(200, json.dumps({"ok": True, "want": want, "have": have, "index_sha": META["index_sha"]}), "application/json")

    def do_HEAD(self): self.do_GET()
    def _forbid(self): return self._send(403, "view-only") if self._authed() else self._unauth(urlparse(self.path).path)
    do_PUT = do_DELETE = do_PATCH = do_OPTIONS = _forbid

    def do_GET(self):
        u = urlparse(self.path); p = u.path
        if p == "/_alive": return self._send(200, "ok")
        if p == "/login": return self._send(302, "", extra=[("Location", "/")]) if self._authed() else self._login_page()
        if p == "/api/links": return self._send(200, json.dumps(META["links"] or {}), "application/json")
        if p == "/logout":
            return self._send(302, "", extra=[("Location", "/login"), ("Set-Cookie", f"{COOKIE}=; Max-Age=0; Path=/; HttpOnly; Secure; SameSite=Lax")])
        if p == "/favicon.ico": return self._send(404, "")
        who = self._authed()
        if not who: return self._unauth(p)
        if p in ("/", "/index.html"): return self._send(200, render_index(), "text/html; charset=utf-8")
        if p == "/healthz":
            return self._send(200, json.dumps({"ok": True, "paper_only": True, "mirror": True, "board_age_s": key_age("/api/board"),
                                               "box_heartbeat_age_s": (time.time() - META["hb"]) if META["hb"] else None}), "application/json")
        if p == "/api/mirror":
            return self._send(200, json.dumps({"version": META["version"], "box_heartbeat_age_s": (time.time() - META["hb"]) if META["hb"] else None,
                                               "pushes": META["pushes"], "restored": META["restored"], "keys": {k: round(key_age(k) or 0, 1) for k in SNAP}}), "application/json")
        k = norm_key(p, u.query)
        if not k: return self._send(403, "view-only: path not allowed")
        with COND:
            WANT[k] = time.time()
            s = SNAP.get(k)
            if s is None:
                deadline = time.time() + 12
                while s is None and time.time() < deadline:
                    COND.wait(deadline - time.time()); s = SNAP.get(k)
            age = key_age(k)
        if s is None:
            return self._send(503, json.dumps({"error": "snapshot not available yet (engine box offline or still loading)"}), "application/json")
        return self._send(200, s["gz"], s["ct"], extra=[("X-SPCX-Snap-Age", "%.1f" % age)], gz=True)

if __name__ == "__main__":
    restore()
    threading.Thread(target=persist_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()
    print(f"options flow board mirror on 0.0.0.0:{PORT} (view-only; guest password {'loaded' if GUEST in USERS else 'not set'}; KV {'on' if KV_URL else 'off'}; keepalive {KEEPALIVE})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
