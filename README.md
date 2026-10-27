# Options Flow Board (Render mirror)

Always-on, **view-only** mirror of the Options Flow paper-trading board (all tickers). The engine itself runs on the engine box; the box's
`render_pusher.py` pushes snapshots of the board's read-only GET endpoints (`/api/board`, `/api/version`, `/api/config`,
`/api/state?ticker=..`, `/api/minutes?ticker=..`) plus `static/index.html` to `POST /_push` (bearer token) every ~3 s.

- Same login as the box's `view_proxy.py`: `abhinav` + `SPCX_VIEW_PASSWORD`; `guest` + `SPCX_GUEST_PASSWORD` only while guest
  access is switched on on the box (`guest_on.sh` / `guest_off.sh`, propagated within a few seconds by the pusher).
- No write/trade endpoints: every POST except `/login` and the token-protected `/_push` returns 403.
- Freshness: every JSON reply carries `X-SPCX-Snap-Age`; the board shows "Live · updated Ns ago" or, past 30 s, a "Stale snapshot"
  banner and a STALE badge with the IST time of the last update.
- Tickers other than the default (SPCX) are pushed on demand: the first view of a ticker waits up to ~6 s for the box to deliver it.
- The last core snapshot is kept in a free Render Key Value (`SPCX_KV_URL`) so a cold start still shows the last board.
- No secrets, no trading database in this repo. Stdlib-only Python.
