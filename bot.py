#!/usr/bin/env python3
"""
bygens copy-trade bot - ONE file: Telegram listener + Playwright trader + web dashboard.

  python bot.py             -> run the bot (this is what Render runs). Open its web page to log in to
                               Telegram once; the channel is found automatically from its invite link.

Every step first checks the real state of the website (are we already holding the token? is the
auto-sell still pending? is the sell sheet already open?) and skips what's already done, so it can
resume safely after a crash, a restart, or a trade you opened yourself.
"""
import asyncio
import base64
import collections
import hmac
import itertools
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, UserAlreadyParticipantError
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import ChatInviteAlready


def need(k):
    v = os.environ.get(k)
    if not v:
        sys.exit(f"Missing environment variable: {k}")
    return v


# ------------------------------------------------------------------ config
API_ID, API_HASH = int(need("TG_API_ID")), need("TG_API_HASH")
CHANNEL = os.environ.get("CHANNEL", "").strip()        # optional override; normally auto-detected
if CHANNEL:
    CHANNEL = int(CHANNEL) if CHANNEL.lstrip("-").isdigit() else CHANNEL
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "https://t.me/+KVYWoTt2Tk5kMjQ0")
ENV_SESSION = bool(os.environ.get("TG_SESSION"))
SITE_EMAIL, SITE_PASSWORD = need("SITE_EMAIL"), need("SITE_PASSWORD")
UI_PASSWORD = need("UI_PASSWORD")
E = os.environ
try:
    TZ = ZoneInfo(E.get("TZ_NAME", "Africa/Lagos"))
except Exception:                                      # tz database missing: Lagos is fixed UTC+1
    TZ = timezone(timedelta(hours=1))
DRY_RUN = E.get("DRY_RUN", "1") == "1"
MAX_STAKE = float(E.get("MAX_STAKE", "0"))            # 0 = full balance
MIN_STAKE = 1000.0
STOP_LOSS_PCT = float(E.get("STOP_LOSS_PCT", "3"))
MY_RANK = E.get("MY_RANK", "IRON").strip()               # only act on calls whose header names this rank
RECYCLE = float(E.get("BROWSER_RECYCLE_MINUTES", "20")) * 60   # restart the browser now and then (free-tier memory)
REFRESH = float(E.get("REFRESH_SECONDS", "45"))      # how often the watch re-checks the real dashboard
IDLE_CHECK = float(E.get("IDLE_CHECK_SECONDS", "600"))
STOP_VALUE = float(E.get("STOP_VALUE", "0"))          # >0: sell only when the position is worth this much (₦)
SETTINGS_FILE = "/tmp/settings.json"
POLL = float(E.get("POLL_SECONDS", "2"))
DAY_OFFSETS = {int(x) for x in E.get("ALLOWED_DAY_OFFSETS", "0").split(",")}
CATCHUP_MIN = int(E.get("CATCHUP_MINUTES", "20"))     # after a restart, still act on calls this recent
PORT = int(E.get("PORT", "10000"))
DASH = "https://bygens.app/dashboard"
NAIRA = r"₦\s*([\d,]+(?:\.\d+)?)"
USED_FILE = "/tmp/used_orders_live.json"        # only REAL (live) orders are remembered across restarts

# ------------------------------------------------------------------ shared state (shown in the UI)
S = {"status": "starting", "paused": False, "busy": False, "trade": None, "live": None,
     "started": time.time(), "tg": "connecting", "tg_err": "", "tg_session": None, "channel": None,
     "hands_off": False, "balance": None, "balance_t": None, "logins": 0, "launches": 0, "launched_at": time.time(), "tok_fail": 0}
try:                                                      # dashboard value survives soft restarts
    S["stop_value"] = float(json.load(open(SETTINGS_FILE))["stop_value"])
except Exception:
    S["stop_value"] = STOP_VALUE
S["qty"] = 0


def stop_label():
    v = S["stop_value"]
    return f"when position worth ₦{v:.0f}" if v else f"-{STOP_LOSS_PCT}%"


TG = {}
CHAN = None
tg_ready = asyncio.Event()
_handlers_added = False
RUNS = collections.deque(maxlen=15)
LOGS = collections.deque(maxlen=300)
SHOTS = collections.deque(maxlen=12)
_ids = itertools.count(1)
queue = asyncio.Queue()
force = asyncio.Event()
client = TelegramClient(StringSession(os.environ.get("TG_SESSION", "")), API_ID, API_HASH)
pw = browser = ctx = page = None
try:
    used = set(json.load(open(USED_FILE)))
except Exception:
    used = set()
used_dry = set()                                       # dry-run orders: forgotten on restart


class Fatal(Exception):
    """Don't retry this."""


class StepFailed(Exception):
    pass


def now_s():
    return datetime.now(TZ).strftime("%H:%M:%S")


def log(msg, lvl="info"):
    LOGS.append({"t": now_s(), "lvl": lvl, "msg": msg})
    print(f"[{lvl}] {msg}", flush=True)


async def notify(msg):
    log(msg, "notify")
    try:
        await client.send_message("me", "🤖 " + msg)
    except Exception as e:
        log(f"telegram notify failed: {e}", "warn")


async def shot(name):
    try:
        data = await page.screenshot(type="jpeg", quality=55)
        SHOTS.appendleft({"id": next(_ids), "name": name, "t": now_s(), "data": data})
    except Exception:
        pass


def num(s):
    return float(s.replace(",", "").replace(" ", ""))


# ------------------------------------------------------------------ step tracking
def new_run(title, record=True):
    r = {"id": next(_ids), "title": title, "t": now_s(), "status": "running", "steps": []}
    if record:
        RUNS.appendleft(r)
    return r


def finish(run):
    if run["status"] == "running":
        run["status"] = "ok"


class St:
    def __init__(self, rec):
        self.rec = rec

    def note(self, s):
        self.rec["detail"] = str(s)[:300]

    def skip(self, s):
        self.rec["status"] = "skipped"
        self.rec["detail"] = str(s)[:300]


async def do(run, name, fn, tries=2):
    rec = {"name": name, "status": "running", "detail": "", "t": now_s()}
    run["steps"].append(rec)
    st, err = St(rec), None
    for i in range(1, tries + 1):
        rec["status"] = "running"
        try:
            r = await fn(st)
            if rec["status"] == "running":
                rec["status"] = "ok"
            if rec["detail"].startswith("attempt"):
                rec["detail"] += " (recovered)"
            return r
        except asyncio.CancelledError:
            raise
        except Fatal as e:
            err = e
            rec["detail"] = str(e)[:300]
            break
        except Exception as e:
            err = e
            rec["detail"] = f"attempt {i}/{tries}: {e}"[:300]
            log(f"{name}: {rec['detail']}", "warn")
            await shot(name)
            await asyncio.sleep(1.5)
    rec["status"] = "failed"
    run["status"] = "failed"
    await shot("FAILED " + name)
    raise StepFailed(f"{name}: {err}")


# ------------------------------------------------------------------ browser helpers
async def launch_browser(keep_session=False):
    global pw, browser, ctx, page
    state = None
    if keep_session and ctx:
        try:
            state = await ctx.storage_state()          # cookies etc., so recycling doesn't log you out
        except Exception:
            state = None
    from playwright.async_api import async_playwright
    if pw is None:
        pw = await async_playwright().start()
    try:
        if browser:
            await browser.close()
    except Exception:
        pass
    browser = await pw.chromium.launch(
        headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                             "--renderer-process-limit=1", "--disable-extensions",
                             "--disable-background-networking", "--mute-audio"])
    extra = {"storage_state": state} if state else {}
    ctx = await browser.new_context(**pw.devices["Pixel 7"], **extra)   # layout was captured on mobile

    async def block(route):                                    # save RAM on the free tier
        if route.request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    await ctx.route("**/*", block)
    page = await ctx.new_page()
    S["launches"] += 1
    S["launched_at"] = time.time()
    log("browser recycled" if keep_session else "browser launched")


async def alive():
    try:
        await page.evaluate("1")
        return True
    except Exception:
        return False


async def vis(text, exact=False, t=1500):
    try:
        await page.get_by_text(text, exact=exact).first.wait_for(state="visible", timeout=t)
        return True
    except Exception:
        return False


async def wait_enabled(loc, secs):
    end = time.time() + secs
    while time.time() < end:
        try:
            if await loc.first.is_enabled(timeout=500):
                return
        except Exception:
            pass
        await asyncio.sleep(0.5)
    raise RuntimeError("button not enabled in time")


async def sheet_text():
    d = page.locator('[role="dialog"]:visible')
    if await d.count() == 0:
        raise RuntimeError("no sheet open")
    return await d.last.inner_text()


async def open_dashboard():
    """Open the dashboard and wait until it has really loaded (logging in if the site asks)."""
    if not await alive():
        await launch_browser()
    elif time.time() - S["launched_at"] > RECYCLE:
        await launch_browser(keep_session=True)           # fresh browser, same login
    await page.goto(DASH, wait_until="domcontentloaded")
    for _ in range(20):                                   # the site redirects to login a moment late
        await page.wait_for_timeout(1000)
        if "/auth/login" in page.url:
            break
        try:
            if "Total Balance" in await page.locator("body").inner_text():
                return
        except Exception:
            pass
    if "/auth/login" not in page.url:
        raise RuntimeError("dashboard didn't load in 20s")
    await page.get_by_placeholder("name@example.com").fill(SITE_EMAIL)
    await page.locator('input[type="password"]').fill(SITE_PASSWORD)
    await page.get_by_role("button", name="Sign in").click()
    await page.wait_for_url("**/dashboard**", timeout=20000)
    await page.get_by_text("Total Balance").first.wait_for(timeout=15000)
    S["logins"] += 1
    log("logged in")


HOLD_RE = re.compile(r"([\d,]*\.?\d+)\s+([A-Za-z0-9$]+)\s*\(\s*([\d,]*\.?\d+)\s*\+\s*([\d,]*\.?\d+)\s*locked\s*\)(?:\s*₦\s*([\d,]+(?:\.\d+)?))?")
HOLD_RE2 = re.compile(r"^\s*([\d,]*\.?\d+)\s+([A-Z][A-Z0-9$]+)\s*$", re.M)


def parse_holdings(body):
    hs = [{"symbol": m[1], "avail": num(m[2]), "locked": num(m[3]), "value": num(m[4]) if m[4] else None}
          for m in HOLD_RE.findall(body)]
    if not hs:
        hs = [{"symbol": m[1], "avail": num(m[0]), "locked": 0.0, "value": None} for m in HOLD_RE2.findall(body)]
    return hs


def parse_balance(body):
    m = re.search(r"Total Balance\s*₦\s*([\d,]+\s*\.?\s*\d*)", body)
    try:
        return num(re.sub(r"\s", "", m.group(1))) if m else None
    except Exception:
        return None


async def holdings():
    """Ground truth from the dashboard: list of tokens we hold. [] = nothing."""
    await open_dashboard()
    empty_seen = 0
    for _ in range(12):
        body = await page.locator("body").inner_text()
        b = parse_balance(body)
        if b is not None:
            S["balance"], S["balance_t"] = b, time.time()
        if "No token holdings yet" in body:
            empty_seen += 1                       # must be seen twice: guards against a half-loaded page
            if empty_seen >= 4:
                return []
        else:
            hs = parse_holdings(body)
            if hs:
                return hs
        await asyncio.sleep(1.2)
    raise RuntimeError("couldn't read holdings from dashboard")


async def verified_closed(sym, bal0, pos_val):
    """True only if the token is really gone AND the balance rose by roughly the position's value."""
    await asyncio.sleep(5)
    hs = await holdings()
    if [x for x in hs if x["symbol"].lower() == sym.lower()]:
        return False
    b = S["balance"]
    if bal0 is not None and b is not None and pos_val and (b - bal0) < 0.8 * pos_val:
        log("Dashboard shows no holdings but the balance didn't rise: probably a half-loaded page, "
            "still watching.", "warn")
        return False
    return True


async def open_token(sym):
    await open_dashboard()
    pat = re.compile(rf"[\d.,]+\s*{re.escape(sym)}", re.I)
    last = None
    for _ in range(3):
        try:
            loc = page.get_by_text(pat).first
            await loc.evaluate("e => e.scrollIntoView({block: 'start'})", timeout=8000)
            try:
                await loc.click(timeout=4000)
            except Exception:
                await loc.dispatch_event("click")          # works even when something covers the row
            await page.locator('[role="dialog"]:visible').first.wait_for(timeout=8000)
            await page.get_by_text("Pending Orders", exact=True).first.wait_for(timeout=6000)
            return
        except Exception as e:
            last = e
            await asyncio.sleep(1.5)
    raise last


async def read_balance():
    """Fresh balance as a float, or None if it couldn't be read."""
    try:
        await open_dashboard()
        b = parse_balance(await page.locator("body").inner_text())
        if b is not None:
            S["balance"], S["balance_t"] = b, time.time()
        return b
    except Exception:
        return None


# ------------------------------------------------------------------ BUY
async def process_call(order_id):
    run = new_run(f"Copy order {order_id[:8]}")
    ctxd = {}
    S["status"] = "buying"
    try:
        async def s_check(st):
            hs = await holdings()
            st.note(f"Already holding {hs[0]['symbol']}" if hs else "No open position")
            return hs
        hs = await do(run, "Check for an open position", s_check)
        if hs:
            finish(run)
            log("Already in a trade, skipping buy and managing it.")
            await manage(hs[0])
            return

        async def s_open(st):
            if await vis("Paste order ID here", t=800) or await vis("Take Profit Price", t=300):
                return st.skip("Copy Trade sheet already open")
            await page.get_by_text("Copy Trade", exact=True).first.click()
            await page.get_by_placeholder("Paste order ID here").wait_for(timeout=8000)
        await do(run, "Open Copy Trade", s_open)

        async def s_load(st):
            if await vis("Take Profit Price", t=500):
                return st.skip("order already loaded")
            await page.get_by_placeholder("Paste order ID here").fill(order_id)
            await page.get_by_role("button", name="Load Order").click()
            try:
                await page.get_by_text("Take Profit Price").wait_for(timeout=10000)
            except Exception:
                raise RuntimeError("order didn't load (expired / invalid / already used?)")
        await do(run, "Enter order ID and load it", s_load)

        async def s_read(st):
            mx, text = 0.0, ""
            for _ in range(10):                      # the sheet can show ₦0 for a moment before the balance loads
                text = await sheet_text()
                mx = num(re.findall(NAIRA, text.split("How much NGN", 1)[-1])[-1])
                if mx > 0:
                    break
                await asyncio.sleep(1)
            if mx <= 0 and S["balance"]:
                mx = S["balance"]                    # fall back to the balance read from the dashboard
                st.note("sheet still showed ₦0, using the dashboard balance")
            ctxd["max"] = mx
            ctxd["price"] = num(re.search(r"Current Price:\s*" + NAIRA, text).group(1))
            after = text.split("copy a take profit trade", 1)[-1].strip().splitlines()
            ctxd["symbol"] = after[0].strip()
            st.note(f"{ctxd['symbol']} at ₦{ctxd['price']}, available ₦{ctxd['max']}")
        await do(run, "Read token, price and balance", s_read)

        async def s_stake(st):
            stake = math.floor(ctxd["max"])
            if MAX_STAKE:
                stake = min(stake, int(MAX_STAKE))
            if stake < MIN_STAKE:
                raise Fatal(f"Balance ₦{ctxd['max']} is below the ₦{MIN_STAKE:.0f} minimum")
            ctxd["stake"] = stake
            inp = page.locator("input:visible").last
            if re.sub(r"\D", "", (await inp.input_value()).split(".")[0]) == str(stake):
                return st.skip(f"₦{stake} already entered")
            await inp.click()
            await inp.fill(str(stake))
            if re.sub(r"\D", "", (await inp.input_value()).split(".")[0]) != str(stake):
                if MAX_STAKE and ctxd["max"] > MAX_STAKE:
                    raise Fatal("Typing failed and slider would exceed MAX_STAKE")
                await page.locator('input[type=range], [role=slider]').first.focus()
                await page.keyboard.press("End")
                st.note("typed input failed, used slider at 100%")
            else:
                st.note(f"typed ₦{stake}")
            try:                                             # remember the take-profit target for the dashboard
                await asyncio.sleep(0.8)
                tp_in = await page.locator("input:visible").first.input_value()
                ctxd["tp_price"] = num(re.sub(r"[^\d.,]", "", tp_in))
                m = re.search(r"≈\s*₦\s*([\d,]+(?:\.\d+)?)\s*when executed", await sheet_text())
                ctxd["tp_value"] = num(m.group(1)) if m else None
            except Exception:
                pass
        await do(run, "Enter stake amount", s_stake)

        async def s_confirm(st):
            if not await vis("Take Profit Price", t=500):
                return st.skip("sheet already closed (trade was submitted)")
            btn = page.get_by_role("button", name="Copy Trade").last
            await wait_enabled(btn, 6)
            if DRY_RUN:
                await shot("dry-run ready to confirm")
                await page.get_by_role("button", name="Cancel").click()
                ctxd["dry"] = True
                return st.note("DRY RUN: stopped before the final tap")
            await btn.click()
            await page.get_by_text("Take Profit Price").wait_for(state="hidden", timeout=15000)
        await do(run, "Tap Copy Trade (confirm)", s_confirm, tries=1)
        if ctxd.get("dry"):
            finish(run)
            await notify(f"DRY RUN OK: would stake ₦{ctxd['stake']} on {ctxd['symbol']} at ₦{ctxd['price']}")
            return

        async def s_verify(st):
            for _ in range(10):
                m = [h for h in await holdings() if h["symbol"].lower() == ctxd["symbol"].lower()]
                if m:
                    st.note(f"holding {m[0]['avail'] + m[0]['locked']} {m[0]['symbol']}")
                    return m[0]
                await asyncio.sleep(2)
            raise RuntimeError("position not visible after confirming")
        h = await do(run, "Verify position opened", s_verify, tries=1)
        finish(run)
        S["trade"] = {"symbol": h["symbol"], "entry": ctxd["price"], "stake": ctxd["stake"],
                      "opened": now_s(), "baseline": False,
                      "tp_price": ctxd.get("tp_price"), "tp_value": ctxd.get("tp_value")}
        await notify(f"BOUGHT {h['symbol']}: ₦{ctxd['stake']} at ₦{ctxd['price']}. Stop-loss {stop_label()}.")
        await manage(h)
    except Exception as e:
        run["status"] = "failed"
        S["status"] = "error"
        await notify(f"BUY FAILED: {e}. Re-checking account state...")
        await reconcile("after failed buy")


# ------------------------------------------------------------------ MANAGE (watch + stop-loss)
async def manage(h):
    sym = h["symbol"]
    S["status"] = f"holding {sym}"
    run = new_run(f"Manage {sym}")
    t = S["trade"] if S["trade"] and S["trade"]["symbol"].lower() == sym.lower() else None

    async def s_open(st):
        try:
            txt = await sheet_text()
            if sym.lower() in txt.lower() and "Pending Orders" in txt:
                return st.skip("token sheet already open")
        except Exception:
            pass
        await open_token(sym)
    await do(run, "Open token page", s_open)

    async def s_base(st):
        nonlocal t
        price = num(re.search(NAIRA, await sheet_text()).group(1))
        if t:
            return st.note(f"entry ₦{t['entry']}")
        t = S["trade"] = {"symbol": sym, "entry": price, "stake": None, "opened": now_s(), "baseline": True}
        st.note(f"Trade pre-dates the bot, so ₦{price} (price now) is used as the entry for the stop-loss")
    await do(run, "Record entry price", s_base)

    bal0 = S["balance"]

    async def s_tp(st):
        if t.get("tp_price") and t.get("tp_value"):
            return st.note(f"target ₦{t['tp_price']}, you'd get about ₦{t['tp_value']}")
        try:
            await page.get_by_text("Pending Orders", exact=True).click()
            await page.wait_for_timeout(1200)
            txt = await sheet_text()
            m = re.search(r"Auto-sell.*?₦\s*([\d,]+(?:\.\d+)?)\s*≈\s*₦\s*([\d,]+(?:\.\d+)?)", txt, re.S)
            if m:
                t["tp_price"], t["tp_value"] = num(m.group(1)), num(m.group(2))
                st.note(f"target ₦{t['tp_price']}, you'd get about ₦{t['tp_value']}")
            else:
                st.note("couldn't find an auto-sell order (is the take-profit still pending?)")
        except Exception as e:
            st.note(f"couldn't read the target: {e}")
    await do(run, "Read take-profit target", s_tp)

    async def watch(st):
        qty = h["avail"] + h["locked"]
        S["qty"] = qty
        mode = {"v": None, "floor": None}
        S["live"] = {"price": None, "floor": None, "updated": None, "changed": time.time(),
                     "verified": time.time(), "handsoff": False}
        last, closed, errs = None, 0, 0
        next_refresh = time.time() + REFRESH
        while True:
            if force.is_set():
                return "manual"
            if S["hands_off"]:                                   # you're using the app: don't touch the site
                S["live"]["handsoff"] = True
                await asyncio.sleep(POLL)
                continue
            try:
                if time.time() >= next_refresh or S.pop("force_refresh", False):
                    next_refresh = time.time() + REFRESH
                    n0, l0 = S["logins"], S["launches"]
                    hs = await holdings()                        # reloads dashboard; logs in again if kicked out
                    S["live"]["verified"] = time.time()
                    if S["logins"] != n0:
                        if S["launches"] != l0:
                            await notify("The bot's browser had crashed (probably out of memory on the free plan). "
                                         "Restarted it and logged in again.")
                        else:
                            await notify("The site had logged the bot out (you logged in elsewhere?). Logged back in. "
                                         "Note: this logs you out of the app on your other devices.")
                    mine = [x for x in hs if x["symbol"].lower() == sym.lower()]
                    if not mine:
                        if await verified_closed(sym, bal0, (S["live"].get("price") or 0) * qty):
                            return "closed"
                        mine = [{}]                              # false alarm: carry on watching
                    try:
                        await open_token(sym)
                        S["tok_fail"] = 0
                    except Exception as e:
                        S["tok_fail"] += 1
                        n = S["tok_fail"]
                        if n in (1, 5) or n % 20 == 0:
                            log(f"couldn't reopen the token page (x{n} in a row): {str(e)[:120]}. "
                                f"Using the dashboard value as the price.", "warn")
                        if n == 1:
                            await shot("token page won't open")
                        v = mine[0].get("value")
                        if v and qty > 0:
                            S["live"]["price"], S["live"]["updated"] = v / qty, time.time()
                            if mode["floor"] and v / qty <= mode["floor"]:
                                return f"stop-loss (dashboard value): ₦{v / qty:.2f} <= ₦{mode['floor']:.2f}"
                        continue
                txt = await sheet_text()
                price = num(re.search(NAIRA, txt).group(1))
                L = S["live"]
                L["price"], L["updated"], L["handsoff"] = price, time.time(), False
                if price != last:
                    L["changed"], last = time.time(), price
                errs = 0
                v = S["stop_value"]
                if mode["v"] is None or mode["v"] != v:          # decide again whenever you change it
                    if v and qty > 0 and qty * price > v:
                        mode.update(v=v, floor=v / qty)
                        st.note(f"sells if position falls to ₦{v:.0f} (price ₦{v / qty:.2f})")
                    else:
                        mode.update(v=v, floor=t["entry"] * (1 - STOP_LOSS_PCT / 100))
                        st.note(f"sells at -{STOP_LOSS_PCT}% (price ₦{mode['floor']:.2f})")
                        if v:
                            await notify(f"Stop value ₦{v:.0f} is not below this position's worth "
                                         f"(₦{qty * price:.0f}), so -{STOP_LOSS_PCT}% is used instead. "
                                         f"Lower it on the dashboard.")
                L["floor"] = mode["floor"]
                hold = re.search(r"Your Holdings\s*([\d,]*\.?\d+)", txt)
                gone = "Locked in Orders" not in txt and hold and num(hold.group(1)) == 0
                closed = closed + 1 if gone else 0
                if closed >= 3:
                    return "closed"
                if price <= mode["floor"]:
                    return f"stop-loss: ₦{price} <= ₦{mode['floor']:.2f}"
            except Exception as e:
                errs += 1
                if errs in (5, 15):
                    log(f"watch read errors x{errs}: {e}", "warn")
                if errs % 5 == 0:
                    try:
                        hs = await holdings()
                        if not [x for x in hs if x["symbol"].lower() == sym.lower()] and \
                                await verified_closed(sym, bal0, (S["live"].get("price") or 0) * qty):
                            return "closed"
                        await open_token(sym)
                    except Exception as e2:
                        log(f"watch recovery failed: {e2}", "warn")
            await asyncio.sleep(POLL)
    reason = await do(run, f"Watch price (stop-loss {stop_label()})", watch, tries=1)
    finish(run)
    S["live"] = None
    if reason == "closed":
        S["trade"], S["status"] = None, "idle"
        b1 = await read_balance()
        msg = f"{sym} position is closed (take-profit filled or sold)."
        if b1 is not None and bal0 is not None:
            d = b1 - bal0
            msg += f" Balance ₦{b1:,.2f} (was ₦{bal0:,.2f}, {d:+,.2f})."
            if t and t.get("stake"):
                msg += f" About ₦{d - t['stake']:+,.2f} compared with your ₦{t['stake']} stake."
        elif b1 is not None:
            msg += f" Balance ₦{b1:,.2f}."
        await notify(msg)
        return
    ok = await exit_position(sym, reason)
    S["trade"] = None
    S["status"] = "idle" if ok else "error"
    force.clear()


# ------------------------------------------------------------------ EXIT
async def exit_position(sym, reason):
    S["status"] = "selling"
    await notify(f"EXIT {sym}: {reason}. Selling now...")
    for attempt in range(1, 6):
        run = new_run(f"Exit {sym}, attempt {attempt} ({reason[:30]})")
        try:
            await sell_steps(run, sym)
            finish(run)
            b1 = await read_balance()
            await notify(f"SOLD {sym}. Balance: " + (f"₦{b1:,.2f}" if b1 is not None else "?"))
            return True
        except Exception as e:
            run["status"] = "failed"
            await notify(f"Sell attempt {attempt}/5 failed: {e}")
            await asyncio.sleep(5)
    await notify("COULD NOT SELL AFTER 5 ATTEMPTS. OPEN THE APP AND SELL MANUALLY NOW.")
    return False


async def sell_steps(run, sym):
    async def s_check(st):
        hs = [h for h in await holdings() if h["symbol"].lower() == sym.lower()]
        if not hs:
            st.note("nothing held, already sold")
        return hs
    if not await do(run, "Check position still open", s_check):
        return

    async def s_open(st):
        try:
            txt = await sheet_text()
            if "Sell Now" in txt or ("Pending Orders" in txt and sym.lower() in txt.lower()):
                return st.skip("sheet already open")
        except Exception:
            pass
        await open_token(sym)
    await do(run, "Open token page", s_open)

    async def s_cancel(st):
        if await vis("Sell Now", exact=True, t=400):
            return st.skip("sell sheet already open")
        await page.get_by_text("Pending Orders", exact=True).click()
        if await vis("No pending orders", t=1500):
            return st.skip("no auto-sell order (already cancelled)")
        for _ in range(3):
            row = page.locator("xpath=//*[contains(text(),'Auto-sell')]/ancestor::div[.//button][1]")
            await row.locator("button:visible").last.click()          # the X
            if await vis("No pending orders", t=4000):
                st.note("auto-sell cancelled")
                return
        raise RuntimeError("auto-sell still listed after 3 taps on X")
    await do(run, "Cancel auto-sell order (X)", s_cancel)

    async def s_tap(st):
        if await vis("Sell Now", exact=True, t=500):
            return st.skip("sell sheet already open")
        btn = page.get_by_role("button", name="Sell", exact=True)
        await wait_enabled(btn, 12)
        await btn.click()
        await page.get_by_role("button", name="Sell Now").wait_for(timeout=8000)
    await do(run, "Tap Sell", s_tap)

    async def s_amount(st):
        txt = await sheet_text()
        if "(All)" in txt:
            return st.skip("already set to 100%")
        await page.locator('input[type=range], [role=slider]').first.focus()
        await page.keyboard.press("End")
        txt = await sheet_text()
        if "(All)" in txt:
            return st.note("slider set to 100%")
        mx = re.findall(r"([\d.,]+)\s*" + re.escape(sym), txt, re.I)[-1]
        await page.locator("input:visible").last.fill(mx.replace(",", ""))
        st.note(f"typed max amount {mx}")
    await do(run, "Set amount to 100%", s_amount)

    async def s_confirm(st):
        if not await vis("Sell Now", exact=True, t=500):
            return st.skip("sell sheet already closed")
        await page.get_by_role("button", name="Sell Now").click()
        await page.get_by_role("button", name="Sell Now").wait_for(state="hidden", timeout=15000)
    await do(run, "Tap Sell Now", s_confirm, tries=1)

    async def s_verify(st):
        for _ in range(10):
            if not [h for h in await holdings() if h["symbol"].lower() == sym.lower()]:
                return st.note("no holdings left")
            await asyncio.sleep(2)
        raise RuntimeError("still holding after Sell Now")
    await do(run, "Verify sold", s_verify)


# ------------------------------------------------------------------ RECONCILE (resume from real state)
async def reconcile(why):
    quiet = why == "periodic check"
    run = new_run(f"Check account state ({why})", record=not quiet)
    try:
        async def s(st):
            hs = await holdings()
            st.note(", ".join(f"{h['symbol']} (free {h['avail']}, locked {h['locked']})" for h in hs)
                    or "no open position")
            return hs
        hs = await do(run, "Read account state", s, tries=3)
        finish(run)
        if hs:
            await notify(f"Found open position {hs[0]['symbol']}. Resuming watch.")
            await manage(hs[0])
        else:
            S["trade"], S["status"] = None, "idle"
    except Exception as e:
        run["status"] = "failed"
        if quiet:
            RUNS.appendleft(run)
        S["status"] = "error"
        log(f"reconcile failed: {e}", "error")


# ------------------------------------------------------------------ Telegram intake
def parse_call(text):
    m_id = re.search(r"ORDER\s*ID\s*:?\s*([0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})", text)
    if not m_id:
        return None
    m_dt = re.search(r"CALL\s*-?\s*(\d{1,2})/(\d{1,2})/(\d{2,4})", text, re.I)
    return m_id.group(1).lower(), (m_dt.groups() if m_dt else None)


def date_ok(parts):
    if not parts:
        return False
    d, m, y = (int(x) for x in parts)                    # day/month/year
    y += 2000 if y < 100 else 0
    try:
        return (datetime(y, m, d).date() - datetime.now(TZ).date()).days in DAY_OFFSETS
    except ValueError:
        return False


def rank_ok(text):
    """True if the call's header line (e.g. 'IRON AND ABOVE CALL FOR TODAY') names my rank."""
    m = re.search(r"^.*CALL\s+FOR\s+TODAY.*$", text, re.I | re.M)
    line = m.group(0).strip() if m else ""
    return bool(re.search(rf"\b{re.escape(MY_RANK)}\b", line, re.I)), line


def handle_text(text, src="live"):
    text = re.sub(r"[*_`~]", "", text or "")              # ignore bold/italic markers
    p = parse_call(text)
    if not p:
        return
    oid, parts = p
    ids = used_dry if DRY_RUN else used
    if oid in ids:
        if src != "live":
            log(f"Order {oid[:8]} was already handled, ignored", "info")
        return
    ok, header = rank_ok(text)
    if not ok:
        log(f"Skipped order {oid[:8]}: header \"{header or 'not found'}\" is not for rank {MY_RANK}", "warn")
        return
    if not date_ok(parts):
        log(f"Ignored order {oid[:8]} ({src}): call date {parts} is not an allowed date", "warn")
        return
    if S["paused"]:
        log(f"Paused: skipped order {oid[:8]}", "warn")
        return
    if S["hands_off"]:
        log(f"Hands-off mode is ON: skipped order {oid[:8]}", "warn")
        return
    if S["busy"]:
        log(f"Busy or already in a trade: skipped order {oid[:8]}", "warn")
        return
    ids.add(oid)
    if not DRY_RUN:
        try:
            json.dump(sorted(used), open(USED_FILE, "w"))
        except Exception:
            pass
    S["busy"] = True
    log(f"New call ({src}): order {oid[:8]}")
    queue.put_nowait(("call", oid))


async def catchup(minutes=None):
    win = minutes or CATCHUP_MIN
    seen = recent = 0
    try:
        async for m in client.iter_messages(CHAN, limit=15):          # newest first
            seen += 1
            txt = m.raw_text or ""
            if txt and datetime.now(timezone.utc) - m.date < timedelta(minutes=win):
                recent += 1
                handle_text(txt, "catch-up")
                if S["busy"]:
                    break
        log(f"Channel scan: read {seen} messages, {recent} within the last {win} min.")
    except Exception as e:
        log(f"Channel scan failed: {e}", "error")


async def idle_loop():
    """While idle, re-check the account now and then (keeps balance fresh and the login alive)."""
    while True:
        await asyncio.sleep(IDLE_CHECK)
        if tg_ready.is_set() and not S["busy"] and not S["hands_off"] and queue.empty():
            S["busy"] = True
            queue.put_nowait(("reconcile", "periodic check"))


async def worker():
    S["busy"] = True
    try:
        await reconcile("startup")
    finally:
        S["busy"] = not queue.empty()
    await tg_ready.wait()                                   # wait for Telegram login + channel
    if not S["trade"]:
        try:
            await catchup()
        except Exception as e:
            log(f"catch-up failed: {e}", "warn")
    while True:
        job = await queue.get()
        try:
            if job[0] == "call":
                await process_call(job[1])
            else:
                await reconcile(job[1])
        except Exception as e:
            log(f"job crashed: {e}", "error")
            S["status"] = "error"
        finally:
            S["busy"] = not queue.empty()
            if S["status"] in ("buying", "selling") or S["status"].startswith("holding"):
                S["status"] = "idle"



# ------------------------------------------------------------------ Telegram login + channel detection
async def resolve_channel():
    if CHANNEL:
        return CHANNEL
    h = CHANNEL_LINK.rstrip("/").split("+")[-1].split("joinchat/")[-1]
    inv = await client(CheckChatInviteRequest(h))
    if isinstance(inv, ChatInviteAlready):
        return inv.chat                                     # you are already a member
    try:
        upd = await client(ImportChatInviteRequest(h))
        return upd.chats[0]
    except UserAlreadyParticipantError:
        raise RuntimeError("already a member but couldn't resolve the channel; set CHANNEL manually")
    except Exception as e:
        if "InviteRequestSent" in type(e).__name__:
            raise RuntimeError("Join request sent. Wait for the channel admin to approve it, then press Retry.")
        raise


async def on_post(ev):
    handle_text(ev.raw_text)


async def on_cmd(ev):
    c = (ev.raw_text or "").strip().lower()
    if c == "/pause":
        S["paused"] = True
        await notify("Paused.")
    elif c == "/resume":
        S["paused"] = False
        await notify("Resumed.")
    elif c == "/sell":
        force.set()
        await notify("Force sell requested.")
    elif c == "/status":
        await notify(f"status={S['status']} paused={S['paused']} dry_run={DRY_RUN}")


async def start_listening():
    global CHAN, _handlers_added
    try:
        CHAN = await resolve_channel()
        if not _handlers_added:
            client.add_event_handler(on_post, events.NewMessage(chats=CHAN))
            client.add_event_handler(on_post, events.MessageEdited(chats=CHAN))
            client.add_event_handler(on_cmd, events.NewMessage(chats="me"))
            _handlers_added = True
        S["channel"] = getattr(CHAN, "title", str(CHAN))
        S["tg"], S["tg_err"] = "ready", ""
        tg_ready.set()
        await notify(f"Bot online. {'DRY RUN' if DRY_RUN else 'LIVE'}, stop-loss {stop_label()}, "
                     f"stake {'full balance' if not MAX_STAKE else '₦' + str(MAX_STAKE)}. "
                     f"Listening to: {S['channel']}")
    except Exception as e:
        S["tg"], S["tg_err"] = "channel_error", f"{type(e).__name__}: {e}"
        log(f"channel setup failed: {e}", "error")


async def after_login():
    S["tg_err"] = ""
    if not ENV_SESSION:
        S["tg_session"] = client.session.save()
    await start_listening()

# ------------------------------------------------------------------ web dashboard
PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Copy-trade bot</title>
<style>
:root{--bg:#0b0d10;--card:#14181d;--line:#232a32;--tx:#e8edf2;--mut:#8a96a3;--ok:#2fd13a;--bad:#ff5c5c;--warn:#f5b942;--blue:#5aa9ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.4 system-ui,sans-serif;padding:12px;max-width:760px;margin:auto}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px;margin-bottom:12px}
h2{font-size:13px;letter-spacing:.06em;text-transform:uppercase;color:var(--mut);margin:0 0 8px}
.pill{display:inline-block;padding:4px 12px;border-radius:99px;font-weight:700;font-size:13px}
.pill.ok{background:#12351a;color:var(--ok)}.pill.idle{background:#222a33;color:var(--mut)}.pill.bad{background:#3a1717;color:var(--bad)}.pill.warn{background:#3a2d10;color:var(--warn)}
.mut{color:var(--mut)}.kv{display:flex;justify-content:space-between;padding:3px 0}
button{background:#1d242c;color:var(--tx);border:1px solid var(--line);border-radius:10px;padding:9px 14px;font-size:14px;margin-right:6px}
button.red{border-color:#6b2a2a;color:var(--bad)}
details{border-top:1px solid var(--line);padding:8px 0}summary{cursor:pointer}
.step{display:flex;gap:8px;padding:4px 0 0 4px}.step .nm{flex:1}.step .tm{color:var(--mut);font-size:12px}
.step.failed .nm{color:var(--bad)}.step.skipped .nm{color:var(--mut)}.det{margin:0 0 4px 30px;font-size:13px;color:var(--mut)}
.step.failed+.det{color:var(--bad)}.log{font:12px/1.5 ui-monospace,monospace;padding:1px 0}.log.warn{color:var(--warn)}.log.error{color:var(--bad)}.log.notify{color:var(--blue)}
a{color:var(--blue)}.err{color:var(--bad);margin:6px 0;font-size:13px}
</style></head><body>
<div class="card"><span id="pill" class="pill idle">...</span> <span id="badges" class="mut" style="font-size:13px"></span>
<div style="margin-top:10px"><button id="bpause">Pause</button><button id="brecheck">Re-check account</button><button id="bscan">Re-scan channel</button><button id="bhands">I'm using the app</button><button id="bsell" class="red">Force sell</button></div></div>
<div class="card" id="tgcard" style="display:none"></div>
<div class="card"><h2>Stop-loss</h2><div class="mut" id="slnow" style="margin-bottom:6px"></div>
<input id="slin" type="number" inputmode="numeric" placeholder="Sell if my position falls to ₦ (e.g. 2000)" style="width:100%;padding:10px;margin:4px 0 8px;border-radius:10px;border:1px solid var(--line);background:#0b0d10;color:var(--tx);font-size:15px">
<button id="slsave">Save</button><button id="sloff">Use % instead</button><div id="slmsg" class="mut" style="margin-top:6px"></div></div>
<div class="card"><h2>Position</h2><div id="pos"></div></div>
<div class="card"><h2>Steps (newest first)</h2><div id="runs"></div></div>
<div class="card"><h2>Screenshots</h2><div id="shots" class="mut"></div></div>
<div class="card"><h2>Log</h2><div id="logs"></div></div>
<script>
const $=s=>document.querySelector(s);const ICON={ok:'✅',failed:'❌',skipped:'⏭️',running:'⏳'};
const openIds=new Set(),seen=new Set();let last=null;
function el(t,c,x){const e=document.createElement(t);if(c)e.className=c;if(x!==undefined)e.textContent=x;return e}
function fmt(s){s=Math.round(s);if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';return Math.floor(s/3600)+'h '+Math.floor(s%3600/60)+'m'}
async function api(p,post){const r=await fetch(p,{method:post?'POST':'GET'});return r.json()}
function kv(k,v,c){const d=el('div','kv');d.append(el('span','mut',k),el('b',c||'',v));return d}
let tgKey='';
function renderTg(s){const key=s.tg+'|'+(s.tg_session||'')+'|'+(s.tg_err||'');if(key===tgKey)return;tgKey=key;
 const c=$('#tgcard');c.replaceChildren();const show=s.tg!=='ready'||s.tg_session;c.style.display=show?'block':'none';if(!show)return;
 c.append(el('h2','','Telegram'));if(s.tg_err)c.append(el('div','err',s.tg_err));
 const inp=(ph,t)=>{const i=el('input');i.placeholder=ph;i.type=t||'text';i.style.cssText='width:100%;padding:10px;margin:6px 0;border-radius:10px;border:1px solid var(--line);background:#0b0d10;color:var(--tx);font-size:15px';return i};
 const post=async(p,b)=>{await fetch(p,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b||{})});tick()};
 if(s.tg==='login_needed'){c.append(el('div','mut','Phone number with country code, e.g. +2348012345678. Telegram sends the login code inside your Telegram app.'));
  const i=inp('+234...','tel'),b=el('button','','Send code');b.onclick=()=>post('/api/tg/phone',{phone:i.value});c.append(i,b)}
 else if(s.tg==='code_sent'){c.append(el('div','mut','Enter the code Telegram sent you (check your Telegram app).'));
  const i=inp('12345','tel'),b=el('button','','Log in');b.onclick=()=>post('/api/tg/code',{code:i.value});c.append(i,b)}
 else if(s.tg==='need_password'){c.append(el('div','mut','Enter your Telegram two-step verification password.'));
  const i=inp('password','password'),b=el('button','','Log in');b.onclick=()=>post('/api/tg/password',{password:i.value});c.append(i,b)}
 else if(s.tg==='channel_error'){const b=el('button','','Retry');b.onclick=()=>post('/api/tg/retry');c.append(b)}
 else if(s.tg==='connecting'){c.append(el('div','mut','Connecting...'))}
 if(s.tg_session){c.append(el('div','mut','Logged in. To survive restarts, paste this into Render > Environment > TG_SESSION and save (keep it secret):'));
  const ta=el('textarea');ta.readOnly=true;ta.value=s.tg_session;ta.style.cssText='width:100%;height:90px;margin-top:6px;background:#0b0d10;color:var(--tx);border:1px solid var(--line);border-radius:10px;font:12px monospace';ta.onclick=()=>ta.select();c.append(ta)}}
let slInit=false;
function renderSl(s){const w=s.pos_value?' (worth ₦'+s.pos_value.toFixed(0)+' now)':'';
 $('#slnow').textContent=s.stop_value?('Sell when your position falls to ₦'+s.stop_value+w):('No value set: selling at -'+s.pct+'% from entry'+w);
 if(!slInit){$('#slin').value=s.stop_value||'';slInit=true}}
async function saveStop(v){const r=await fetch('/api/stop',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({value:v})});
 const d=await r.json();const m=$('#slmsg');m.className=d.ok?'mut':'err';m.textContent=d.ok?'Saved.':d.error;tick()}
function render(s){last=s;renderTg(s);renderSl(s);const p=$('#pill');p.textContent=s.status.toUpperCase();
 p.className='pill '+(s.status==='error'?'bad':s.status==='idle'?'idle':s.status==='selling'?'warn':'ok');
 $('#badges').textContent=(s.dry?'DRY RUN':'LIVE')+' · rank '+s.rank+' · stop-loss '+s.sl+' · up '+fmt(s.uptime)+(s.paused?' · PAUSED':'')+' · Telegram: '+(s.tg==='ready'?(s.channel||'ready'):s.tg)+(s.balance!==null?' · Balance ₦'+s.balance.toFixed(2)+' ('+fmt(s.balance_age)+' ago)':'')+(s.hands_off?' · HANDS OFF':'');
 $('#bhands').textContent=s.hands_off?'Resume bot (I am done)':'I\'m using the app';
 $('#bpause').textContent=s.paused?'Resume':'Pause';
 const pos=$('#pos');pos.replaceChildren();const t=s.trade,L=s.live;
 if(!t){pos.append(el('div','mut','No open position'))}else{
  pos.append(kv('Token',t.symbol),kv('Stake',t.stake?'₦'+t.stake:'(opened before bot)'),kv('Entry','₦'+t.entry+(t.baseline?' (baseline)':'')));
  if(t.tp_price){const g=(t.stake&&t.tp_value)?t.tp_value-t.stake:null;
   pos.append(kv('Sells at (target)','₦'+t.tp_price),kv('You get at target','₦'+(t.tp_value||'?')+(g!==null?' ('+(g>=0?'+':'')+g.toFixed(2)+')':''),'color:var(--ok)'))}
  if(L&&L.price!==null){const dp=((L.price/t.entry-1)*100).toFixed(2);
   pos.append(kv('Price now','₦'+L.price+' ('+dp+'%)',dp<0?'color:var(--bad)':''),kv('Stop-loss at','₦'+L.floor.toFixed(2)+(s.qty?'  (you get ~₦'+(L.floor*s.qty).toFixed(0)+')':'')),
   kv('Price last changed',fmt(L.changed_ago)+' ago',L.changed_ago>600?'color:var(--warn)':''),kv('Checked on the site',fmt(L.verified_ago)+' ago',L.verified_ago>180?'color:var(--warn)':''))}}
 const runs=$('#runs');runs.replaceChildren();
 s.runs.forEach(r=>{if(!seen.has(r.id)){seen.add(r.id);if(r.status!=='ok')openIds.add(r.id)}
  const d=el('details');d.open=openIds.has(r.id);d.addEventListener('toggle',()=>{d.open?openIds.add(r.id):openIds.delete(r.id)});
  const sm=el('summary');sm.append(el('span','',(ICON[r.status]||'•')+' '),el('b','',r.title),el('span','mut','  '+r.t));d.append(sm);
  r.steps.forEach(x=>{const row=el('div','step '+x.status);row.append(el('span','',ICON[x.status]||'•'),el('span','nm',x.name),el('span','tm',x.t));d.append(row);
   if(x.detail)d.append(el('div','det',x.detail))});runs.append(d)});
 if(!s.runs.length)runs.append(el('div','mut','Nothing yet. Waiting for a call.'));
 const sh=$('#shots');sh.replaceChildren();s.shots.forEach(x=>{const a=el('a','',x.t+'  '+x.name);a.href='/shot/'+x.id;a.target='_blank';sh.append(a,el('br'))});
 if(!s.shots.length)sh.textContent='None yet';
 const lg=$('#logs');lg.replaceChildren();s.logs.forEach(x=>lg.append(el('div','log '+x.lvl,x.t+'  '+x.msg)))}
async function tick(){try{render(await api('/api/state'))}catch(e){$('#pill').textContent='OFFLINE';$('#pill').className='pill bad'}}
$('#slsave').onclick=()=>saveStop(parseFloat($('#slin').value||0));
$('#sloff').onclick=()=>{$('#slin').value='';saveStop(0)};
$('#bhands').onclick=async()=>{await fetch('/api/handsoff',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({on:!(last&&last.hands_off)})});tick()};
$('#bscan').onclick=async()=>{await api('/api/rescan',true);tick()};
$('#bpause').onclick=async()=>{await api(last&&last.paused?'/api/resume':'/api/pause',true);tick()};
$('#brecheck').onclick=async()=>{await api('/api/recheck',true);tick()};
$('#bsell').onclick=async()=>{if(confirm('Cancel the auto-sell and sell everything now?')){await api('/api/sell',true);tick()}};
tick();setInterval(tick,2500);
</script></body></html>"""


def build_app():
    from aiohttp import web

    @web.middleware
    async def auth(request, handler):
        if request.path == "/health":
            return web.Response(text="ok")
        ok = False
        h = request.headers.get("Authorization", "")
        if h.startswith("Basic "):
            try:
                ok = hmac.compare_digest(base64.b64decode(h[6:]).decode().partition(":")[2], UI_PASSWORD)
            except Exception:
                pass
        if not ok:
            return web.Response(status=401, headers={"WWW-Authenticate": 'Basic realm="bot"'})
        return await handler(request)

    async def index(_):
        return web.Response(text=PAGE, content_type="text/html")

    async def state(_):
        L = dict(S["live"]) if S["live"] else None
        if L:
            L["changed_ago"] = time.time() - L["changed"]
            L["updated_ago"] = time.time() - (L["updated"] or time.time())
            L["verified_ago"] = time.time() - L.get("verified", time.time())
        return web.json_response({
            "status": S["status"], "paused": S["paused"], "dry": DRY_RUN, "sl": stop_label(), "stop_value": S["stop_value"], "pct": STOP_LOSS_PCT,
            "pos_value": (S["qty"] * L["price"]) if (L and L.get("price")) else None,
            "uptime": time.time() - S["started"], "trade": S["trade"], "live": L,
            "hands_off": S["hands_off"], "balance": S["balance"],
            "balance_age": (time.time() - S["balance_t"]) if S["balance_t"] else None,
            "rank": MY_RANK, "qty": S["qty"], "tg": S["tg"], "tg_err": S["tg_err"], "tg_session": S["tg_session"], "channel": S["channel"],
            "runs": list(RUNS), "logs": list(LOGS)[-80:][::-1],
            "shots": [{k: v for k, v in s.items() if k != "data"} for s in SHOTS]})

    async def pause(_):
        S["paused"] = True
        log("Paused from dashboard")
        return web.json_response({"ok": True})

    async def resume(_):
        S["paused"] = False
        log("Resumed from dashboard")
        return web.json_response({"ok": True})

    async def sell(_):
        force.set()
        log("Force sell requested from dashboard", "warn")
        return web.json_response({"ok": True})

    async def recheck(_):
        if S["busy"]:
            log("Re-check ignored: bot is busy / watching a trade", "warn")
        else:
            S["busy"] = True
            queue.put_nowait(("reconcile", "manual re-check"))
        return web.json_response({"ok": True})

    async def jbody(request):
        try:
            return await request.json()
        except Exception:
            return {}

    async def tg_phone(request):
        phone = str((await jbody(request)).get("phone", "")).strip().replace(" ", "")
        try:
            r = await client.send_code_request(phone)
            TG.update(phone=phone, hash=r.phone_code_hash)
            S["tg"], S["tg_err"] = "code_sent", ""
        except Exception as e:
            S["tg_err"] = f"{type(e).__name__}: {e}"
        return web.json_response({"ok": True})

    async def tg_code(request):
        code = str((await jbody(request)).get("code", "")).strip().replace(" ", "")
        try:
            await client.sign_in(TG["phone"], code, phone_code_hash=TG["hash"])
            await after_login()
        except SessionPasswordNeededError:
            S["tg"], S["tg_err"] = "need_password", ""
        except Exception as e:
            S["tg_err"] = f"{type(e).__name__}: {e}"
        return web.json_response({"ok": True})

    async def tg_password(request):
        try:
            await client.sign_in(password=str((await jbody(request)).get("password", "")))
            await after_login()
        except Exception as e:
            S["tg_err"] = f"{type(e).__name__}: {e}"
        return web.json_response({"ok": True})

    async def tg_retry(_):
        if await client.is_user_authorized():
            await start_listening()
        return web.json_response({"ok": True})

    async def rescan(_):
        if S["busy"] or S["trade"] or not tg_ready.is_set():
            log("Re-scan ignored: bot is busy, holding a trade, or Telegram isn't ready", "warn")
        else:
            log("Re-scanning the channel (last 60 minutes)...")
            asyncio.create_task(catchup(60))
        return web.json_response({"ok": True})

    async def handsoff(request):
        on = bool((await jbody(request)).get("on"))
        S["hands_off"] = on
        if not on:
            S["force_refresh"] = True
        log("Hands-off ON: the bot won't touch the site" if on else "Hands-off OFF: the bot resumes", "notify")
        return web.json_response({"ok": True})

    async def set_stop(request):
        try:
            v = float((await jbody(request)).get("value") or 0)
        except Exception:
            return web.json_response({"ok": False, "error": "Enter a number, e.g. 2000"})
        if v < 0:
            return web.json_response({"ok": False, "error": "Can't be negative"})
        L = S["live"]
        if v and L and L.get("price") and S["qty"] and v >= S["qty"] * L["price"]:
            return web.json_response({"ok": False, "error": (
                f"₦{v:.0f} is not below your position's current worth (₦{S['qty'] * L['price']:.0f}). "
                f"It would sell immediately. Pick a lower number.")})
        S["stop_value"] = v
        try:
            json.dump({"stop_value": v}, open(SETTINGS_FILE, "w"))
        except Exception:
            pass
        log(f"Stop-loss changed from dashboard: {stop_label()}", "notify")
        return web.json_response({"ok": True})

    async def get_shot(request):
        i = int(request.match_info["i"])
        for s in SHOTS:
            if s["id"] == i:
                return web.Response(body=s["data"], content_type="image/jpeg")
        return web.Response(status=404)

    app = web.Application(middlewares=[auth])
    app.add_routes([web.get("/", index), web.get("/health", index), web.get("/api/state", state),
                    web.post("/api/pause", pause), web.post("/api/resume", resume),
                    web.post("/api/sell", sell), web.post("/api/recheck", recheck), web.post("/api/stop", set_stop), web.post("/api/handsoff", handsoff), web.post("/api/rescan", rescan),
                    web.post("/api/tg/phone", tg_phone), web.post("/api/tg/code", tg_code),
                    web.post("/api/tg/password", tg_password), web.post("/api/tg/retry", tg_retry),
                    web.get("/shot/{i}", get_shot)])
    return app


# ------------------------------------------------------------------ main
async def main():
    from aiohttp import web
    runner = web.AppRunner(build_app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()          # open the port first so Render is happy
    log(f"dashboard on port {PORT}")
    await launch_browser()
    await client.connect()
    asyncio.create_task(worker())
    asyncio.create_task(idle_loop())
    if await client.is_user_authorized():
        await start_listening()
    else:
        S["tg"] = "login_needed"
        log("Telegram login needed: open the dashboard and log in", "warn")
    while True:                                   # stay alive; Telethon delivers updates in the background
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
