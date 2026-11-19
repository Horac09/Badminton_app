#!/usr/bin/env python3
"""
Badminton Brno - přehled volných kurtů
  * TJ Tesla Brno – Lesná (Reenio)
  * Xarena Slatina (Bizzy / e-rezervace.cz)
  * Centrum Viktoria (Clubspire timeline)
  * Ivanovice (Memberzone)

Spuštění:
    pip install requests tzdata
    python badminton_brno.py                             # web (http://localhost:8765 nebo jiný volný port)
    python badminton_brno.py --cli                       # výpis volných slotů do konzole
    python badminton_brno.py --export site               # statický web pro mobil (GitHub Pages)
    python badminton_brno.py --dump                      # surová data Reenio -> reenio_dump.json
    python badminton_brno.py --dump-xarena 25.11.2026    # surové HTML Xarena -> xarena_dump*.html
    python badminton_brno.py --dump-viktoria 25.11.2026
    python badminton_brno.py --dump-memberzone 01.12.2026
"""
import argparse, json, re, threading, time, html as htmlmod, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs, urlencode
import requests

# ---------------------------------------------------------------- KONFIGURACE
REFRESH_MIN = 10          # obnova dat na pozadí (min)
DAYS_AHEAD = 14           # kolik dní dopředu načítat (výchozí)
MAX_WORKERS = 8           # kolik dotazů běží zároveň celkem
PER_HOST = 3              # výchozí max. souběžných spojení na jeden web
RETRIES = 3               # kolikrát zkusit den znovu při výpadku spojení
MAX_FAILS_IN_ROW = 3      # po tolika neúspěšných dnech za sebou web v tomto kole vzdáme
GRID_STEP_MIN = 30        # řádek mřížky = 30 min

VENUES = [
    {
        "name": "TJ Tesla Brno – Lesná",
        "base": "https://tjteslabrno.reenio.cz",
        "section": "/cs/view/lesna-f1/sportovni-hala-f290/badminton-f292",
        # ID kurtu (číslo za "-r" v URL) -> (název, poslední část URL kurtu)
        "resources": {
            105438: ("Badminton I", "badminton-i-sportovni-hala-badminton-hriste-i-r105438"),
            105439: ("Badminton II", "badminton-ii-sportovni-hala-badminton-hriste-ii-r105439"),
        },
    },
]

XARENA = {
    "name": "Xarena Slatina",
    "url": "https://xarena.e-rezervace.cz/Branch/pages/Schedule.faces",
    "service_id": "40106",        # výběr služby badminton
    "days_ahead": 14,             # rezervace dnes + 14 dní
    "parallel": 3,
    "cookie": "",                 # volitelně JSESSIONID přihlášeného prohlížeče (není potřeba)
}

VIKTORIA = {
    "name": "Centrum Viktoria",
    "base": "https://rezervace.centrumviktoria.cz:18443",
    "sport_index": 1,      # tc[1].s=true = badminton
    "sport_count": 5,      # tc[0]..tc[4]
    "days_ahead": 14,      # rezervace do dnes + 14 dní (23:59)
    "parallel": 1,         # server shazuje souběžná spojení -> jen jedno najednou
    "delay": 1.0,          # pauza mezi dny (s)
    "enabled": True,       # False = nenačítat (např. na firemní síti/VPN, která Viktorii blokuje)
}

MEMBERZONE = {
    "name": "Ivanovice (Memberzone)",
    "url": "https://memberzone.cz/ivanovice/",
    "sport_name": "Badminton",    # tlačítko sportu na stránce
    "days_ahead": 14,
    "parallel": 3,
}

# pořadí sloupců v mřížce
VENUE_ORDER = [v["name"] for v in VENUES] + [XARENA["name"], VIKTORIA["name"], MEMBERZONE["name"]]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36")

_host_locks = {}
def host_slot(host, n=PER_HOST):
    return _host_locks.setdefault(host, threading.BoundedSemaphore(n))

NET_ERRORS = (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError)

def _short(e):
    """Čitelný popis chyby (např. 'ConnectionResetError 10054 …')."""
    s = str(e)
    m = re.search(r"(\w+Error)\((\d+),\s*'([^']+)'", s)
    if m:
        hint = " – spojení blokuje síť (VPN/firewall?)" if m.group(2) == "10054" else ""
        return f"{m.group(1)} {m.group(2)}: {m.group(3)}{hint}"
    return (e.__class__.__name__ + ": " + s)[:200]

def fetch_days(make_client, fetch, days, label, delay=0.0):
    """
    Stáhne dny jedním klientem.
      * výpadek spojení -> počká a zkusí den znovu s novým klientem (max. RETRIES pokusů)
      * MAX_FAILS_IN_ROW neúspěšných dnů za sebou -> zbytek dnů přeskočí (šetří server)
    Vrací (výsledky, chyby).
    """
    client, out, errs = make_client(), [], []
    fails_in_row = 0
    for i, d in enumerate(days):
        ok = False
        for attempt in range(1, RETRIES + 1):
            try:
                out.append(fetch(client, d))
                ok = True
                break
            except NET_ERRORS as e:
                if attempt == RETRIES:
                    errs.append(f"{label} {d:%d.%m.}: {_short(e)} (po {RETRIES} pokusech)")
                else:
                    time.sleep(2.0 * attempt)
                    client = make_client()
            except Exception as e:
                errs.append(f"{label} {d:%d.%m.}: {_short(e)}")
                client = make_client()           # po chybě serveru začít s čistou session
                break
        fails_in_row = 0 if ok else fails_in_row + 1
        if fails_in_row >= MAX_FAILS_IN_ROW and i + 1 < len(days):
            errs.append(f"{label}: {MAX_FAILS_IN_ROW} dny za sebou selhaly – zbylých "
                        f"{len(days) - i - 1} dní v tomto kole přeskočeno")
            break
        if delay:
            time.sleep(delay)
    return out, errs

# ---------------------------------------------------------------- ADAPTÉR REENIO
class ReenioClient:
    def __init__(self, venue, slug):
        self.v, self.slug = venue, slug
        self.s = requests.Session()
        self.s.headers.update({"user-agent": UA, "accept-language": "cs,en;q=0.9"})
        self.version = "3248"

    def page(self):
        return f'{self.v["base"]}{self.v["section"]}/{self.slug}'

    def _init_session(self):
        r = self.s.get(self.page(), timeout=20)
        r.raise_for_status()
        m = re.search(r'reenio[-_]?version["\']?\s*[:=]\s*["\']?(\d+)', r.text, re.I)
        if m:
            self.version = m.group(1)

    def _headers(self, date):
        return {
            "accept": "application/json, text/plain, */*",
            "origin": self.v["base"],
            "referer": f'{self.page()}/{date};viewMode=7-days',
            "x-requested-with": "XMLHttpRequest",
            "x-reenio-version": self.version,
            "x-xsrf-token": self.s.cookies.get("XSRF-TOKEN", ""),
        }

    def term_list(self, resource_id, date):
        if "XSRF-TOKEN" not in self.s.cookies:
            self._init_session()
        fields = {"date": date, "viewMode": "7-days", "page": 0,
                  "filter.pseudoTerm.resourceIds[0]": resource_id,
                  "includeColors": "false", "findNearestAvailable": "false"}
        files = {k: (None, str(v)) for k, v in fields.items()}
        url = self.v["base"] + "/cs/api/Term/List"
        r = self.s.post(url, files=files, headers=self._headers(date), timeout=20)
        if r.status_code in (400, 401, 403):
            self.s.cookies.clear(); self._init_session()
            r = self.s.post(url, files=files, headers=self._headers(date), timeout=20)
        r.raise_for_status()
        return r.json()

# ---------------------------------------------------------------- ADAPTÉR BIZZY / E-REZERVACE (Xarena)
class XarenaClient:
    """
    JSF + RichFaces. Akce kalendáře nepřihlášeného = j_id209, výběr služby = j_id214.
    Odpověď j_id209 obsahuje jen oblast rozvrhu (bez políčka s datem) -> den = požadovaný den.
    """
    SUBMIT_CANDIDATES = ["scheduleNavigForm:j_id209", "scheduleNavigForm:j_id211",
                         "scheduleNavigForm:j_id204"]

    def __init__(self, cfg=XARENA):
        self.c = cfg
        self.submit_id = self.SUBMIT_CANDIDATES[0]
        self.select_name = "scheduleNavigForm:j_id214"
        self.last_html = ""
        self._new_session()

    def _new_session(self):
        self.s = requests.Session()
        self.s.headers.update({"user-agent": UA, "accept-language": "cs,en;q=0.9"})
        if self.c.get("cookie"):
            self.s.cookies.set("JSESSIONID", self.c["cookie"], domain="xarena.e-rezervace.cz")
        self.view_state = None
        self.first_html = ""

    @staticmethod
    def _vs(html):
        m = (re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html) or
             re.search(r'value="([^"]+)"[^>]*name="javax\.faces\.ViewState"', html))
        return m.group(1) if m else None

    def _learn_select(self, html):
        for m in re.finditer(r'<select[^>]*name="(scheduleNavigForm:j_id\d+)"(.*?)</select>', html, re.S):
            if f'value="{self.c["service_id"]}"' in m.group(2):
                self.select_name = m.group(1)
                return

    def _init_session(self):
        r = self.s.get(self.c["url"], timeout=20)
        r.raise_for_status()
        self.first_html = r.text
        self.view_state = self._vs(r.text)
        self._learn_select(r.text)

    @staticmethod
    def _returned_date(html):
        m = re.search(r'schedule_calendarInputDate"[^>]*value="(\d{1,2})\.(\d{1,2})\.(\d{4})"', html)
        return dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else None

    @staticmethod
    def _has_schedule(html):
        return "scheduleTimeHeader" in html and "horizontalRowHeader" in html

    @staticmethod
    def _mentions(html, date):
        pats = [f"{date.day}.{date.month}.{date.year}", f"{date:%d.%m.%Y}", f"{date.day}. {date.month}. {date.year}"]
        return any(p in html for p in pats)

    def _post(self, date, submit_id):
        if not self.view_state:
            self._init_session()
        data = {
            "AJAXREQUEST": "scheduleNavigForm:schedule-navig-region",
            "scheduleNavigForm:schedule_calendarInputDate": date.strftime("%d.%m.%Y"),
            "scheduleNavigForm:schedule_calendarInputCurrentDate": date.strftime("%m/%Y"),
            self.select_name: self.c["service_id"],
            "scheduleNavigForm:view_filter_menu": "horizontal_service_day",
            "scheduleNavigForm_SUBMIT": "1",
            "javax.faces.ViewState": self.view_state or "",
            submit_id: submit_id,
        }
        r = self.s.post(self.c["url"], data=data, timeout=20, headers={
            "Accept": "*/*", "Origin": "https://xarena.e-rezervace.cz", "Referer": self.c["url"],
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        r.raise_for_status()
        return r.text

    def fetch(self, date):
        """Vrací (den, html). Ověří, že odpověď obsahuje rozvrh a případné datum sedí."""
        tried = []
        for sid in [self.submit_id] + [c for c in self.SUBMIT_CANDIDATES if c != self.submit_id]:
            for fresh in (False, True):
                if fresh:
                    self._new_session()
                html = self._post(date, sid)
                self.last_html = html
                got = self._returned_date(html)
                if got == date or (got is None and self._has_schedule(html)):
                    self.submit_id = sid
                    return date, html
                info = got if got else ("rozvrh bez data" if self._has_schedule(html)
                                        else f"bez rozvrhu, {len(html)} znaků")
                tried.append(f"{sid.split(':')[1]}{'+nová session' if fresh else ''} -> {info}")
        raise RuntimeError(f"Xarena nevrátila den {date:%d.%m.%Y} (zkoušeno: {'; '.join(tried)})")

# ---------------------------------------------------------------- ADAPTÉR CENTRUM VIKTORIA (Clubspire timeline)
class ViktoriaClient:
    """POST /timeline/day: tc[i].s = zaškrtnutý sport, criteriaTimestamp = den (ms), _csrf = token ze stránky.
    Server zavírá keep-alive spojení -> každý požadavek posílá 'Connection: close' (nové spojení)."""
    def __init__(self, cfg=VIKTORIA):
        self.c = cfg
        self.s = requests.Session()
        self.s.headers.update({"user-agent": UA, "accept-language": "cs,en;q=0.9", "Connection": "close"})
        self.csrf = None
        self.first_html = ""

    @staticmethod
    def _csrf(html):
        m = (re.search(r'name="_csrf"[^>]*value="([^"]+)"', html) or
             re.search(r'value="([^"]+)"[^>]*name="_csrf"', html) or
             re.search(r'<meta[^>]*name="_csrf"[^>]*content="([^"]+)"', html) or
             re.search(r'<meta[^>]*content="([^"]+)"[^>]*name="_csrf"', html))
        return m.group(1) if m else None

    def _init_session(self):
        r = self.s.get(self.c["base"] + "/timeline/day", timeout=20)
        r.raise_for_status()
        self.first_html = r.text
        self.csrf = self._csrf(r.text)

    @staticmethod
    def _ts(date):
        """Den -> ms epoch (poledne místního času)."""
        noon = dt.datetime.combine(date, dt.time(12, 0))
        if TZ:
            return int(noon.replace(tzinfo=TZ).timestamp() * 1000)
        return int((noon - dt.timedelta(hours=1)).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)

    def fetch(self, date):
        if not self.csrf:
            self._init_session()
        data = []
        for i in range(self.c["sport_count"]):
            if i == self.c["sport_index"]:
                data.append((f"tc[{i}].s", "true"))
            data.append((f"_tc[{i}].s", "on"))
        data += [("criteriaTimestamp", str(self._ts(date))), ("_csrf", self.csrf or "")]
        r = self.s.post(self.c["base"] + "/timeline/day", data=data, timeout=20, headers={
            "Accept": "*/*", "Origin": self.c["base"], "Referer": self.c["base"] + "/timeline/day",
            "X-Ajax-call": "true", "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        r.raise_for_status()
        self.csrf = self._csrf(r.text) or self.csrf
        return r.text

# ---------------------------------------------------------------- ADAPTÉR MEMBERZONE (Ivanovice, ASP.NET WebForms + DevExpress)
class MemberzoneClient:
    """
    1) GET stránky, 2) postback tlačítkem sportu (Badminton), 3) postback datepickeru s novým dnem.
    Každá odpověď = celá stránka (~75 kB) s novým __VIEWSTATE.

    Server vracel HTTP 500 pro dny v jiném měsíci (1.12.+), i když je lze rezervovat.
    Pravděpodobná příčina: stav kalendáře (visibleDate, DATUM_HF). Proto se zkouší
    několik variant formuláře; funkční varianta se zapamatuje.
    """
    DP = "ASPxFormLayout2$Datepicker2"
    # "prev" = přesně jako prohlížeč: visibleDate/initialVisibleDate = dosud zobrazený den,
    # selectedDates = nový den, vše MM/dd/yyyy s nulami (ověřeno z cURL pro 1.12.)
    VARIANTS = ["prev", "target", "prev+hf", "target+hf"]

    def __init__(self, cfg=MEMBERZONE):
        self.c = cfg
        self.variant = self.VARIANTS[0]
        self.tried = []
        self.last_error_html = ""
        self._new_session()

    def _new_session(self):
        self.s = requests.Session()
        self.s.headers.update({"user-agent": UA, "accept-language": "cs,en;q=0.9"})
        self.html = ""
        self.first_html = ""
        self.sport_html = ""

    @staticmethod
    def _inputs(html):
        """Všechna <input name=… value=…> kromě tlačítek/checkboxů -> dict."""
        out = {}
        for tag in re.findall(r"<input\b[^>]*>", html, re.I):
            n = re.search(r'\bname="([^"]+)"', tag)
            if not n:
                continue
            t = re.search(r'\btype="([^"]+)"', tag)
            if t and t.group(1).lower() in ("submit", "button", "image", "checkbox", "radio"):
                continue
            v = re.search(r'\bvalue="([^"]*)"', tag)
            out[htmlmod.unescape(n.group(1))] = htmlmod.unescape(v.group(1)) if v else ""
        return out

    def _post(self, extra):
        f = self._inputs(self.html)
        f.update(extra)
        r = self.s.post(self.c["url"], data=f, timeout=30, headers={
            "origin": "https://memberzone.cz", "referer": self.c["url"],
            "content-type": "application/x-www-form-urlencoded"})
        if r.status_code >= 500:
            self.last_error_html = r.text
        r.raise_for_status()
        self.html = r.text
        return r.text

    @staticmethod
    def _sport_button(html, sport):
        for tag in re.findall(r"<input\b[^>]*>", html, re.I):
            if 'type="submit"' in tag and f'value="{sport}"' in tag:
                return re.search(r'name="([^"]+)"', tag).group(1)
        return None

    def _init_session(self):
        r = self.s.get(self.c["url"], timeout=30)
        r.raise_for_status()
        self.first_html = self.html = r.text
        btn = self._sport_button(r.text, self.c["sport_name"])
        if btn:
            self._post({"__EVENTTARGET": "", "__EVENTARGUMENT": "", btn: self.c["sport_name"]})
            self.sport_html = self.html

    @staticmethod
    def shown_date(html):
        m = (re.search(r'name="ASPxFormLayout2\$Datepicker2"[^>]*value="(\d{1,2})\.(\d{1,2})\.(\d{4})"', html) or
             re.search(r'name="DATUM_HF"[^>]*value="(\d{1,2})\.(\d{1,2})\.(\d{4})"', html))
        return dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else None

    @staticmethod
    def error_title(html):
        """Z chybové stránky ASP.NET vytáhne popis výjimky."""
        for pat in (r"<title>\s*(.*?)\s*</title>", r"<h2>\s*<i>(.*?)</i>", r"Exception Details:\s*</b>(.*?)<br"):
            m = re.search(pat, html or "", re.S | re.I)
            if m and m.group(1).strip():
                return re.sub(r"<[^>]+>|\s+", " ", m.group(1)).strip()[:200]
        return ""

    def _form(self, date, variant):
        q = "&quot;"
        prev = self.shown_date(self.html) or dt.date.today()
        vis = prev if variant.startswith("prev") else date
        # DevExpress parsuje stav kalendáře striktně jako MM/dd/yyyy -> dny 1–9 musí mít nulu (12/01/2026)
        us = lambda d: f"{d.month:02d}/{d.day:02d}/{d.year}"
        ms = int(dt.datetime.combine(date, dt.time()).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        f = {
            "__EVENTTARGET": self.DP,
            "__EVENTARGUMENT": "",
            self.DP: date.strftime("%d.%m.%Y"),
            self.DP + "$State": f"{{{q}rawValue{q}:{q}{ms}{q},{q}useMinDateInsteadOfNull{q}:false}}",
            self.DP + "$DDD$C": (f"{{{q}visibleDate{q}:{q}{us(vis)}{q},{q}initialVisibleDate{q}:{q}{us(prev)}{q},"
                                 f"{q}selectedDates{q}:[{q}{us(date)}{q}]}}"),
        }
        if variant.endswith("+hf"):
            f["DATUM_HF"] = date.strftime("%d.%m.%Y")
        return f

    def fetch(self, date):
        if not self.html:
            self._init_session()
        if self.shown_date(self.html) == date and self.sport_html:
            return self.html
        self.tried = []
        order = [self.variant] + [v for v in self.VARIANTS if v != self.variant]
        last_exc = None
        for variant in order:
            try:
                html = self._post(self._form(date, variant))
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else "?"
                self.tried.append(f"{variant} -> HTTP {code}")
                last_exc = e
                self._new_session(); self._init_session()    # po chybě serveru čistý start
                continue
            got = self.shown_date(html)
            if got and got != date:
                self.tried.append(f"{variant} -> den {got:%d.%m.}")
                continue
            self.variant = variant
            return html
        detail = self.error_title(self.last_error_html)
        raise RuntimeError(f"Memberzone neukázal {date:%d.%m.%Y} (zkoušeno: {'; '.join(self.tried)})"
                           + (f" – server: {detail}" if detail else "")) from last_exc

# ---------------------------------------------------------------- POMOCNÉ
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Europe/Prague")
except Exception:
    TZ = None

def _dt(s):
    d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is None:
        return d
    if TZ:
        return d.astimezone(TZ).replace(tzinfo=None)
    y = d.year
    last_sun = lambda m: max(dt.date(y, m, x) for x in range(25, 32) if dt.date(y, m, x).weekday() == 6)
    naive = d.replace(tzinfo=None)
    summer = dt.datetime.combine(last_sun(3), dt.time(1)) <= naive < dt.datetime.combine(last_sun(10), dt.time(1))
    return naive + dt.timedelta(hours=2 if summer else 1)

def _merge(iv):
    res = []
    for a, b in sorted(iv):
        if res and a <= res[-1][1]:
            res[-1] = (res[-1][0], max(res[-1][1], b))
        else:
            res.append((a, b))
    return res

def _dur(s, default_h):
    try:
        h, m, sec = (int(x) for x in str(s).split(":"))
        return dt.timedelta(hours=h, minutes=m, seconds=sec)
    except Exception:
        return dt.timedelta(hours=default_h)

def _when(t, now):
    t = max(t, now)
    if t - now < dt.timedelta(minutes=1):
        return "právě teď"
    if t.date() == now.date():
        return f"dnes {t:%H:%M}"
    if t.date() == now.date() + dt.timedelta(days=1):
        return f"zítra {t:%H:%M}"
    return f'{"po út st čt pá so ne".split()[t.weekday()]} {t:%d.%m. %H:%M}'

DOW = "Po Út St Čt Pá So Ne".split()

# ---------------------------------------------------------------- PARSER Reenio
def parse_terms(data, now=None):
    """Bloky podle reservationIntervalSize. free: True=volno, None=rezervace ještě neotevřena, False=obsazeno."""
    now = (now or dt.datetime.now()).replace(second=0, microsecond=0)
    out = []
    fmt = lambda x: x.strftime("%Y-%m-%d %H:%M")
    for ev in (data.get("data") or {}).get("events", []):
        w0, w1 = _dt(ev["start"]), _dt(ev["end"])
        step = dt.timedelta(minutes=ev.get("reservationIntervalSize") or 30)
        busy = [(_dt(r["start"]), _dt(r["end"])) for r in ev.get("reservations") or []]
        for lst in (ev.get("collisions") or {}).values():
            busy += [(_dt(c["start"]), _dt(c["end"])) for c in lst or []]
        busy = _merge([(a, b) for a, b in set(busy) if a < w1 and b > w0])
        price = (ev.get("priceVariants") or [{}])[0].get("price")
        open_before = _dur(ev.get("reservationOpenDate"), 240)
        due_before = _dur(ev.get("reservationDueDate"), 1)
        t = w0
        while t + step <= w1:
            b0, b1 = t, t + step
            t = b1
            if b0 < now + due_before:
                continue
            if any(a < b1 and b > b0 for a, b in busy):
                free, opens = False, None
            elif b0 - open_before <= now:
                free, opens = True, None
            else:
                free, opens = None, _when(b0 - open_before, now)
            out.append({"start": fmt(b0), "end": fmt(b1), "free": free, "opens": opens,
                        "price": price, "block": int(step.total_seconds() // 60)})
    return out

# ---------------------------------------------------------------- PARSER Xarena
def parse_xarena(html, now=None, day=None):
    """Sloupce = hodiny (scheduleTimeHeader), řádky = kurty (horizontalRowHeader),
    obsazenost = scheduleData.events[] se start/end [sloupec, řádek]."""
    now = (now or dt.datetime.now()).replace(second=0, microsecond=0)
    m = re.search(r'schedule_calendarInputDate"[^>]*value="(\d{1,2})\.(\d{1,2})\.(\d{4})"', html)
    if m:
        day = dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    if day is None:
        return []
    times = re.findall(r'class="scheduleTimeHeader[^"]*"[^>]*>\s*(\d{1,2}):(\d{2})\s*<', html)
    courts = re.findall(r'class="horizontalRowHeader[^"]*"[^>]*title="([^"]+)"', html)
    if not times or not courts:
        return []
    sd = re.search(r'var scheduleData = (\{.*?\});\s*//', html, re.S)
    events = json.loads(sd.group(1)).get("events", []) if sd else []
    busy = set()
    for e in events:
        (c0, r0), (c1, r1) = e["start"], e["end"]
        for r in range(min(r0, r1), max(r0, r1) + 1):
            for c in range(min(c0, c1), max(c0, c1) + 1):
                busy.add((c, r))
    out = []
    for r, court in enumerate(courts):
        for c, (hh, mm) in enumerate(times):
            b0 = dt.datetime.combine(day, dt.time(int(hh), int(mm)))
            if b0 < now:
                continue
            out.append({"court": court.strip().capitalize(), "start": b0.strftime("%Y-%m-%d %H:%M"),
                        "end": (b0 + dt.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M"),
                        "free": (c, r) not in busy, "opens": None, "price": None, "block": 60})
    return out

# ---------------------------------------------------------------- PARSER Viktoria
_VK_CELL = re.compile(
    r'<td class="([^"]*)">\s*<div class=\'holder\'>\s*<a class=\'book ajax_popup_trigger\'\s+'
    r'href=\'([^\']*reservationStartTime/(\d{4}-\d\d-\d\dT\d\d:\d\d)[^\']*)\'(.*?)</td>', re.S)

def parse_viktoria(html, now=None):
    """'can_book' = volno, 'full_substitute can_book_substitute' = obsazeno (jen náhradník)."""
    now = (now or dt.datetime.now()).replace(second=0, microsecond=0)
    out = []
    for m in _VK_CELL.finditer(html):
        cls, href, start, rest = m.groups()
        court = re.search(r"lesson_description'>\s*([^<]+?)\s*<", rest)
        price = re.search(r"<strong>(\d+)\s*Kč</strong>\s*/(\d+)\s*min", rest)
        b0 = dt.datetime.strptime(start, "%Y-%m-%dT%H:%M")
        if b0 < now:
            continue
        block = int(price.group(2)) if price else 30
        c = cls.split()
        free = "can_book" in c and not any(x.startswith("full") for x in c)
        out.append({"court": court.group(1) if court else "?", "start": b0.strftime("%Y-%m-%d %H:%M"),
                    "end": (b0 + dt.timedelta(minutes=block)).strftime("%Y-%m-%d %H:%M"),
                    "free": free, "opens": None, "block": block,
                    "price": int(price.group(1)) if price else None})
    return out

# ---------------------------------------------------------------- PARSER Memberzone (Ivanovice)
_MZ_ROW = re.compile(r'<tr id="[^"]*RADEK_DAY_\d+"[^>]*>\s*<td[^>]*class="color_tmava_bcg">([^<]+)</td>(.*?)</tr>', re.S)
_MZ_CELL = re.compile(r'<td id="[^"]*_(\d{3})(\d{2})(\d{2})" colspan="(\d+)">\s*<input\b([^>]*)>', re.S)

def parse_memberzone(html, now=None, day=None, first_hour=6):
    """
    Řádek = kurt ("Badminton 1"), buňka = 30min slot.
    ID buňky: <kurt 3 číslice><hodina, 01 = 6:00><čtvrthodina, 01 = :00, 03 = :30>.
    Volno = <input class="btnrez"> bez disabled; "Blokováno" = obsazeno.
    """
    now = (now or dt.datetime.now()).replace(second=0, microsecond=0)
    m = re.search(r'name="DATUM_HF"[^>]*value="(\d{1,2})\.(\d{1,2})\.(\d{4})"', html)
    if m:
        day = dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    if day is None:
        return []
    out = []
    for court, row in _MZ_ROW.findall(html):
        for _, hidx, q, span, attrs in _MZ_CELL.findall(row):
            b0 = dt.datetime.combine(day, dt.time(first_hour + int(hidx) - 1, (int(q) - 1) * 15))
            if b0 < now:
                continue
            block = int(span) * 15
            free = "disabled" not in attrs and 'class="btnrez"' in attrs
            out.append({"court": court.strip(), "start": b0.strftime("%Y-%m-%d %H:%M"),
                        "end": (b0 + dt.timedelta(minutes=block)).strftime("%Y-%m-%d %H:%M"),
                        "free": free, "opens": None, "price": None, "block": block})
    return out

# ---------------------------------------------------------------- NAČÍTÁNÍ (paralelně, průběžně)
STATE = {"results": {}, "updated": None, "errors": [], "done": 0, "total": 0, "loading": False}
_lock = threading.Lock()

def _days(n_ahead):
    today = dt.date.today()
    return [today + dt.timedelta(days=i) for i in range(n_ahead + 1)]

def _chunks(lst, n):
    size = max(1, -(-len(lst) // n))
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def _report(errs):
    if errs:
        with _lock:
            STATE["errors"].extend(errs)

def _reenio_task(v, rid, court, slug, date):
    def run():
        with host_slot(v["base"]):
            res, errs = fetch_days(lambda: ReenioClient(v, slug), lambda c, d: c.term_list(rid, date),
                                   [dt.date.fromisoformat(date)], f'{v["name"]} {court}')
        if errs:
            raise RuntimeError(errs[0])
        data = res[0]
        def view(now):
            return [{**t, "venue": v["name"], "court": court,
                     "link": f'{v["base"]}{v["section"]}/{slug}/{t["start"][:10]};viewMode=7-days'}
                    for t in parse_terms(data, now)]
        return view
    return (f'{v["name"]}|{rid}|{date}', run)

def _xarena_task(days):
    def run():
        with host_slot("xarena", XARENA["parallel"]):
            pages, errs = fetch_days(XarenaClient, lambda c, d: c.fetch(d), days, XARENA["name"])
        _report(errs)
        def view(now):
            return [{**t, "venue": XARENA["name"], "link": XARENA["url"]}
                    for d, html in pages for t in parse_xarena(html, now, d)]
        return view
    return (f'xarena|{days[0]}', run)

def _viktoria_task(days):
    def run():
        with host_slot("viktoria", VIKTORIA["parallel"]):
            pages, errs = fetch_days(ViktoriaClient, lambda c, d: c.fetch(d), days,
                                     VIKTORIA["name"], VIKTORIA.get("delay", 0))
        _report(errs)
        def view(now):
            link = f'{VIKTORIA["base"]}/timeline/day?tabIdx={VIKTORIA["sport_index"]}&resetFilter=true#timelineCalendar'
            return [{**t, "venue": VIKTORIA["name"], "link": link}
                    for html in pages for t in parse_viktoria(html, now)]
        return view
    return (f'viktoria|{days[0]}', run)

def _memberzone_task(days):
    def run():
        with host_slot("memberzone", MEMBERZONE["parallel"]):
            pages, errs = fetch_days(MemberzoneClient, lambda c, d: (d, c.fetch(d)), days, MEMBERZONE["name"])
        _report(errs)
        def view(now):
            return [{**t, "venue": MEMBERZONE["name"], "link": MEMBERZONE["url"]}
                    for d, html in pages for t in parse_memberzone(html, now, d)]
        return view
    return (f'memberzone|{days[0]}', run)

def build_tasks():
    today = dt.date.today()
    tasks = []
    for v in VENUES:
        for rid, (court, slug) in v["resources"].items():
            for off in range(0, DAYS_AHEAD, 7):
                tasks.append(_reenio_task(v, rid, court, slug, (today + dt.timedelta(days=off)).isoformat()))
    for ch in _chunks(_days(XARENA["days_ahead"]), XARENA["parallel"]):
        tasks.append(_xarena_task(ch))
    if VIKTORIA.get("enabled", True):
        for ch in _chunks(_days(VIKTORIA["days_ahead"]), VIKTORIA["parallel"]):
            tasks.append(_viktoria_task(ch))
    for ch in _chunks(_days(MEMBERZONE["days_ahead"]), MEMBERZONE["parallel"]):
        tasks.append(_memberzone_task(ch))
    return tasks

_collect_lock = threading.Lock()

def collect():
    """Načte vše. Pokud už načítání běží, nic nedělá (ruční a automatická obnova se nepřekrývají)."""
    if not _collect_lock.acquire(blocking=False):
        return False
    try:
        _collect()
    finally:
        _collect_lock.release()
    return True

def refresh_now():
    if _collect_lock.locked():
        return False
    threading.Thread(target=collect, daemon=True).start()
    return True

def _collect():
    tasks = build_tasks()
    with _lock:
        STATE.update(loading=True, done=0, total=len(tasks), errors=[])
    t0 = time.time()
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futs = {ex.submit(run): key for key, run in tasks}
        for f in as_completed(futs):
            key = futs[f]
            with _lock:
                try:
                    STATE["results"][key] = f.result()
                except Exception as e:
                    STATE["errors"].append(f"{key}: {e}")
                STATE["done"] += 1
    valid = {k for k, _ in tasks}
    with _lock:
        for k in list(STATE["results"]):
            if k not in valid:
                del STATE["results"][k]
        STATE.update(loading=False, updated=dt.datetime.now().strftime("%d.%m. %H:%M"),
                     duration=round(time.time() - t0, 1))

def current_slots():
    now = dt.datetime.now()
    with _lock:
        views = list(STATE["results"].values())
    slots = [s for view in views for s in view(now)]
    uniq = {(s["venue"], s["court"], s["start"], s["free"]): s for s in slots}
    return sorted(uniq.values(), key=lambda s: (s["start"], s["venue"], s["court"]))

def refresher():
    while True:
        collect(); time.sleep(REFRESH_MIN * 60)

# ---------------------------------------------------------------- MŘÍŽKA
def build_grid(slots, day, show_later):
    step = dt.timedelta(minutes=GRID_STEP_MIN)
    cells = {}
    for s in slots:
        if not s["start"].startswith(day):
            continue
        if s["free"] is False or (s["free"] is None and not show_later):
            continue
        t = dt.datetime.strptime(s["start"], "%Y-%m-%d %H:%M")
        end = dt.datetime.strptime(s["end"], "%Y-%m-%d %H:%M")
        while t < end:
            cells.setdefault(t.strftime("%H:%M"), {}).setdefault(s["venue"], []).append(s)
            t += step
    return sorted(cells), cells

def _court_key(c):
    return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", c)]

def _cell_html(lst):
    if not lst:
        return "none", "–"
    free_now = [s for s in lst if s["free"] is True]
    later = [s for s in lst if s["free"] is None]
    n = len({s["court"] for s in free_now})
    label = f"{n} volné" if n else ""
    if later:
        label += (" + " if n else "") + f"{len({s['court'] for s in later})} později"
    cls = "c3" if n >= 3 else ("c2" if n == 2 else ("c1" if n == 1 else "clater"))
    items = []
    for s in sorted(lst, key=lambda s: _court_key(s["court"])):
        extra = f' · {s["price"]} Kč/{s["block"]} min' if s.get("price") else ""
        st = "" if s["free"] else f' <i>(rezervace od {s["opens"]})</i>'
        items.append(f'<li><a href="{s["link"]}" target=_blank>{htmlmod.escape(s["court"])}</a>'
                     f' <small>{s["start"][11:]}–{s["end"][11:]}{extra}</small>{st}</li>')
    return cls, f'<details><summary>{label}</summary><ul>{"".join(items)}</ul></details>'

def render_grid(slots, day, show_later):
    """Hodinové bloky se spojí přes 2 řádky (rowspan=2), půlhodinové zůstávají samostatně."""
    times, cells = build_grid(slots, day, show_later)
    venues = list(VENUE_ORDER)
    if not times:
        return "<p>V tento den není nic volného.</p>"
    step = dt.timedelta(minutes=GRID_STEP_MIN)
    nxt = lambda tm: (dt.datetime.strptime(tm, "%H:%M") + step).strftime("%H:%M")
    ident = lambda lst: sorted((s["court"], s["start"], s["end"]) for s in lst)
    span, tset = {}, set(times)
    for v in venues:
        for tm in times:
            if span.get((tm, v)) == 0:
                continue
            lst = cells.get(tm, {}).get(v, [])
            t2 = nxt(tm)
            if (lst and t2 in tset
                    and all(s.get("block") == 2 * GRID_STEP_MIN and s["start"][11:] == tm for s in lst)
                    and ident(lst) == ident(cells.get(t2, {}).get(v, []))):
                span[(tm, v)], span[(t2, v)] = 2, 0
            else:
                span[(tm, v)] = 1
    head = "<tr><th class=tcol>Čas</th>" + "".join(f"<th>{htmlmod.escape(v)}</th>" for v in venues) + "</tr>"
    rows = []
    for tm in times:
        tds = [f"<td class=tcol>{tm}</td>"]
        for v in venues:
            sp = span.get((tm, v), 1)
            if sp == 0:
                continue
            cls, inner = _cell_html(cells.get(tm, {}).get(v, []))
            rs = " rowspan=2" if sp == 2 else ""
            hour = " hour" if sp == 2 else ""
            tds.append(f'<td class="{cls}{hour}"{rs}>{inner}</td>')
        rows.append("<tr>" + "".join(tds) + "</tr>")
    return f"<div class=gridwrap><table class=grid>{head}{''.join(rows)}</table></div>"

def render_list(slots, day, show_later, show_all):
    rows = []
    for s in slots:
        if day and not s["start"].startswith(day): continue
        if s["free"] is False and not show_all: continue
        if s["free"] is None and not (show_later or show_all): continue
        if s["free"] is True:   cls, txt = "free", "VOLNO"
        elif s["free"] is None: cls, txt = "later", f'rezervace od {s["opens"]}'
        else:                   cls, txt = "busy", "obsazeno"
        d = dt.date.fromisoformat(s["start"][:10])
        price = f'{s["price"]} Kč / {s.get("block", 30)} min' if s.get("price") else "–"
        rows.append(f'<tr><td>{DOW[d.weekday()]} {d:%d.%m.}</td>'
                    f'<td>{s["start"][11:]}–{s["end"][11:]}</td><td>{s["venue"]}</td>'
                    f'<td>{s["court"]}</td><td class={cls}>{txt}</td><td>{price}</td>'
                    f'<td><a href="{s["link"]}" target=_blank>Otevřít rezervaci</a></td></tr>')
    return ("<table class=list><tr><th>Den</th><th>Čas</th><th>Hala</th><th>Kurt</th><th>Stav</th>"
            "<th>Cena</th><th></th></tr>" + ("".join(rows) or "<tr><td colspan=7>Nic nenalezeno</td></tr>")
            + "</table>")

# ---------------------------------------------------------------- WEB UI
PAGE = """<!doctype html><html lang=cs><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">%(refresh)s<title>Badminton Brno</title>
<style>
body{font-family:Segoe UI,sans-serif;margin:20px;background:#f5f7fa;color:#222}
h1{color:#1a5e20;margin-bottom:8px}
.days a{display:inline-block;padding:6px 10px;margin:2px;border-radius:6px;background:#fff;
  border:1px solid #ccd;text-decoration:none;color:#222;font-size:14px}
.days a.sel{background:#1a5e20;color:#fff;border-color:#1a5e20}
.bar{margin:10px 0}.bar a{margin-right:12px}
table{border-collapse:collapse;background:#fff;width:100%%}
td,th{padding:6px 10px;border-bottom:1px solid #e3e3e3;text-align:left;vertical-align:top}
table.grid th{background:#1a5e20;color:#fff;position:sticky;top:0}
table.grid td.tcol,table.grid th.tcol{width:60px;font-weight:600}
table.grid td.none{color:#ccc;text-align:center}
td.hour{vertical-align:middle;border-left:3px solid #1a5e20}
td.c1{background:#e8f5e9}td.c2{background:#c8e6c9}td.c3{background:#a5d6a7}td.clater{background:#f1f1f1}
summary{cursor:pointer;font-weight:600;color:#1a5e20}td.clater summary{color:#777;font-weight:400}
details ul{margin:6px 0 0 0;padding-left:18px}details li{margin:2px 0}
.free{color:#1a7f37;font-weight:600}.busy{color:#aaa}.later{color:#888;font-style:italic}
.err{color:#c00;font-size:12px}.errn{color:#c00;font-weight:600;margin-left:8px}.load{color:#b58900}
.btn{display:inline-block;margin-left:12px;padding:5px 12px;border-radius:6px;background:#1a5e20;
  color:#fff;text-decoration:none;font-size:14px}
.btn:hover{background:#2e7d32}.btn.warn{background:#c62828}.btn.warn:hover{background:#e53935}
@media (max-width:700px){
  body{margin:8px;font-size:14px}h1{font-size:20px}
  .days{display:flex;overflow-x:auto;white-space:nowrap;-webkit-overflow-scrolling:touch}
  .days a{flex:0 0 auto;padding:8px 10px}
  .bar a{display:inline-block;margin:4px 8px 4px 0}
  .btn{margin:6px 0 0 0}
  .gridwrap{overflow-x:auto}
  td,th{padding:5px 6px}table.grid th{font-size:12px}
  table.grid td.tcol,table.grid th.tcol{width:44px;position:sticky;left:0;background:#fff;z-index:1}
  table.grid th.tcol{background:#1a5e20}
  summary{font-size:13px}
}
</style>
<h1>🏸 Volné badmintonové kurty – Brno</h1>
<div class=days>%(days)s</div>
<div class=bar>%(toggles)s</div>
<p>%(status)s</p>
%(body)s
%(errs)s</html>"""

def _max_days():
    return max(DAYS_AHEAD, XARENA["days_ahead"] + 1, VIKTORIA["days_ahead"] + 1, MEMBERZONE["days_ahead"] + 1)

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/refresh":
            refresh_now()
            self.send_response(303)
            self.send_header("Location", "/" + (("?" + u.query) if u.query else ""))
            self.end_headers()
            return
        q = parse_qs(u.query)
        today = dt.date.today()
        day = q.get("d", [today.isoformat()])[0]
        view = q.get("v", ["grid"])[0]
        show_later, show_all = "later" in q, "all" in q
        params = {"d": day, "v": view}
        if show_later: params["later"] = 1
        if show_all: params["all"] = 1
        link = lambda **kw: "?" + urlencode({k: v for k, v in {**params, **kw}.items() if v is not None})

        days = []
        for i in range(_max_days()):
            d = today + dt.timedelta(days=i)
            sel = " class=sel" if d.isoformat() == day else ""
            days.append(f'<a{sel} href="{link(d=d.isoformat())}">{DOW[d.weekday()]} {d:%d.%m.}</a>')

        toggles = [
            f'<a href="{link(v="list" if view == "grid" else "grid")}">'
            f'{"📋 Seznam" if view == "grid" else "▦ Mřížka"}</a>',
            f'<a href="{link(later=None if show_later else 1)}">'
            f'{"☑" if show_later else "☐"} i termíny, kde se rezervace teprve otevře</a>',
        ]
        if view == "list":
            toggles.append(f'<a href="{link(all=None if show_all else 1)}">{"☑" if show_all else "☐"} i obsazené</a>')

        slots = current_slots()
        body = render_grid(slots, day, show_later) if view == "grid" else render_list(slots, day, show_later, show_all)

        with _lock:
            loading, done, total = STATE["loading"], STATE["done"], STATE["total"]
            upd, dur, errs = STATE["updated"], STATE.get("duration"), list(STATE["errors"])
        qs = urlencode(params)
        if loading:
            status = f'<span class=load>Načítám… {done}/{total}</span>' + (f' (zobrazena data z {upd})' if upd else '')
        else:
            status = (f'Aktualizováno: {upd or "–"}' + (f' (načtení trvalo {dur} s)' if dur else '')
                      + f' <a class="btn{" warn" if errs else ""}" href="/refresh?{qs}">🔄 Obnovit data</a>')
            if errs:
                status += f'<span class=errn>⚠ {len(errs)} chyb při načítání – zkuste obnovit</span>'
        page = PAGE % {"refresh": '<meta http-equiv=refresh content=3>' if loading else "",
                       "days": "".join(days), "toggles": "".join(toggles), "status": status, "body": body,
                       "errs": "".join(f"<div class=err>{htmlmod.escape(e)}</div>" for e in errs)}
        self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers(); self.wfile.write(page.encode())

class Server(ThreadingMixIn, HTTPServer):
    daemon_threads = True

# ---------------------------------------------------------------- EXPORT (statická PWA pro mobil)
def _icon_png(path, size):
    """Jednoduchá ikona aplikace (zelený čtverec se zkratkou)."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return False
    img = Image.new("RGB", (size, size), (26, 94, 32))
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", int(size * 0.42))
    except OSError:
        font = ImageFont.load_default()
    txt = "BB"
    box = d.textbbox((0, 0), txt, font=font)
    d.text(((size - (box[2] - box[0])) / 2 - box[0], (size - (box[3] - box[1])) / 2 - box[1]),
           txt, fill="white", font=font)
    img.save(path)
    return True

EXPORT_JS = """
function show(d){
  document.querySelectorAll('.day').forEach(e=>e.style.display=e.id==='d'+d?'block':'none');
  document.querySelectorAll('.days a').forEach(a=>a.classList.toggle('sel',a.dataset.d===d));
  history.replaceState(null,'','#'+d);
}
function hidePast(){
  const now=new Date(), p=n=>String(n).padStart(2,'0');
  const today=now.getFullYear()+'-'+p(now.getMonth()+1)+'-'+p(now.getDate());
  const hm=p(now.getHours())+':'+p(now.getMinutes());
  const el=document.getElementById('d'+today);
  if(el) el.querySelectorAll('tr[data-t]').forEach(r=>{ if(r.dataset.t<hm) r.style.display='none'; });
}
document.addEventListener('DOMContentLoaded',()=>{
  const first=document.querySelector('.days a');
  const want=location.hash.slice(1);
  show(document.getElementById('d'+want)?want:first.dataset.d);
  hidePast();
  if('serviceWorker' in navigator) navigator.serviceWorker.register('sw.js');
});
"""

EXPORT_SW = """
const C='badminton-v1';
self.addEventListener('install',e=>{self.skipWaiting();e.waitUntil(caches.open(C).then(c=>c.addAll(['./','index.html','manifest.webmanifest','icon-192.png','icon-512.png'])))});
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
// vždy nejdřív síť (čerstvá data), bez signálu poslední uložená verze
self.addEventListener('fetch',e=>{
  e.respondWith(fetch(e.request).then(r=>{const k=r.clone();caches.open(C).then(c=>c.put(e.request,k));return r})
    .catch(()=>caches.match(e.request)));
});
"""

def export_site(out_dir):
    """Načte všechny haly a uloží statický web (index.html + PWA soubory) do out_dir."""
    import os
    os.makedirs(out_dir, exist_ok=True)
    collect()
    slots = current_slots()
    today = dt.date.today()
    css = re.search(r"<style>(.*?)</style>", PAGE, re.S).group(1).replace("%%", "%")
    tabs, days = [], []
    for i in range(_max_days()):
        d = today + dt.timedelta(days=i)
        ds = d.isoformat()
        tabs.append(f'<a href="#{ds}" data-d="{ds}" onclick="show(\'{ds}\');return false">{DOW[d.weekday()]} {d:%d.%m.}</a>')
        grid = render_grid(slots, ds, False)
        grid = re.sub(r"<tr><td class=tcol>(\d\d:\d\d)</td>", r'<tr data-t="\1"><td class=tcol>\1</td>', grid)
        days.append(f'<div class=day id="d{ds}" style="display:none">{grid}</div>')
    with _lock:
        upd, errs = STATE["updated"], list(STATE["errors"])
    err_html = ""
    if errs:
        err_html = (f'<details class=err><summary>⚠ {len(errs)} chyb při posledním načtení</summary>'
                    + "".join(f"<div>{htmlmod.escape(e)}</div>" for e in errs) + "</details>")
    html = f"""<!doctype html><html lang=cs><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<meta name=theme-color content="#1a5e20">
<meta name=apple-mobile-web-app-capable content=yes>
<meta name=apple-mobile-web-app-title content="Badminton">
<link rel=manifest href="manifest.webmanifest">
<link rel=apple-touch-icon href="icon-192.png">
<title>Badminton Brno</title><style>{css}</style>
<h1>🏸 Volné kurty – Brno</h1>
<div class=days>{''.join(tabs)}</div>
<p>Aktualizováno: {upd} <a class=btn href="#" onclick="location.reload();return false">🔄 Načíst znovu</a></p>
{''.join(days)}
{err_html}
<script>{EXPORT_JS}</script></html>"""
    open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8").write(html)
    json.dump({"name": "Badminton Brno – volné kurty", "short_name": "Badminton",
               "start_url": "./", "display": "standalone", "background_color": "#f5f7fa",
               "theme_color": "#1a5e20",
               "icons": [{"src": "icon-192.png", "sizes": "192x192", "type": "image/png"},
                         {"src": "icon-512.png", "sizes": "512x512", "type": "image/png"}]},
              open(os.path.join(out_dir, "manifest.webmanifest"), "w", encoding="utf-8"), ensure_ascii=False)
    open(os.path.join(out_dir, "sw.js"), "w", encoding="utf-8").write(EXPORT_SW)
    for s in (192, 512):
        _icon_png(os.path.join(out_dir, f"icon-{s}.png"), s)
    open(os.path.join(out_dir, ".nojekyll"), "w").close()
    return len(slots), errs

# ---------------------------------------------------------------- MAIN
def _parse_day(arg):
    return (dt.date.today() + dt.timedelta(days=int(arg[1:])) if arg.startswith("+")
            else dt.datetime.strptime(arg, "%d.%m.%Y").date())

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", action="store_true")
    ap.add_argument("--dump-xarena", nargs="?", const="+1", default=None, help="DD.MM.YYYY nebo +N")
    ap.add_argument("--dump-viktoria", nargs="?", const="+1", default=None, help="DD.MM.YYYY nebo +N")
    ap.add_argument("--dump-memberzone", nargs="?", const="+2", default=None, help="DD.MM.YYYY nebo +N")
    ap.add_argument("--cli", action="store_true")
    ap.add_argument("--export", metavar="SLOZKA", help="uloží statický web (PWA) do složky – pro GitHub Pages")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1",
                    help="0.0.0.0 = dostupné i z jiných zařízení v síti (mobil na stejné Wi-Fi)")
    a = ap.parse_args()
    if a.export:
        n, errs = export_site(a.export)
        print(f"Export hotov: {a.export}/index.html ({n} slotů, {len(errs)} chyb)")
        for e in errs: print("CHYBA:", e)
    elif a.dump_memberzone:
        d = _parse_day(a.dump_memberzone)
        mz = MemberzoneClient()
        html = ""
        try:
            html = mz.fetch(d)
            print(f"OK – funkční varianta: {mz.variant}")
        except Exception as e:
            print("POZOR:", e)
        if mz.tried:
            print("Zkoušené varianty:", "; ".join(mz.tried))
        if mz.last_error_html:
            open("memberzone_error.html", "w", encoding="utf-8").write(mz.last_error_html)
            print(f"Chybová stránka serveru uložena: memberzone_error.html "
                  f"({mz.error_title(mz.last_error_html) or 'bez popisu'})")
        open("memberzone_dump_page.html", "w", encoding="utf-8").write(mz.sport_html or mz.first_html)
        open("memberzone_dump.html", "w", encoding="utf-8").write(html)
        sl = parse_memberzone(html, dt.datetime(2000, 1, 1), d) if html else []
        print(f"Zobrazený den {mz.shown_date(html) if html else '–'} (chtěl jsem {d:%d.%m.%Y}); "
              f"bloků: {len(sl)}, volných: {sum(s['free'] for s in sl)}")
    elif a.dump_viktoria:
        d = _parse_day(a.dump_viktoria)
        vk = ViktoriaClient()
        try:
            html = vk.fetch(d)
        except Exception as e:
            raise SystemExit(f"Chyba: {_short(e)}")
        open("viktoria_dump_page.html", "w", encoding="utf-8").write(vk.first_html)
        open("viktoria_dump.html", "w", encoding="utf-8").write(html)
        n = len(parse_viktoria(html, dt.datetime(2000, 1, 1)))
        print(f"CSRF nalezen: {bool(vk.csrf)}; bloků: {n}; uloženo pro den {d:%d.%m.%Y}")
    elif a.dump_xarena:
        d = _parse_day(a.dump_xarena)
        x = XarenaClient()
        try:
            _, html = x.fetch(d)
        except RuntimeError as e:
            print("POZOR:", e); html = x.last_html
        open("xarena_dump_page.html", "w", encoding="utf-8").write(x.first_html)
        open("xarena_dump.html", "w", encoding="utf-8").write(html)
        print("submit id:", x.submit_id, "| select:", x.select_name)
        n = len(parse_xarena(html, dt.datetime(2000, 1, 1), d)) if html else 0
        print(f"Rozvrh v odpovědi: {x._has_schedule(html)}, bloků: {n}, datum v textu: {x._mentions(html, d)}")
    elif a.dump:
        v = VENUES[0]; rid, (court, slug) = next(iter(v["resources"].items()))
        data = ReenioClient(v, slug).term_list(rid, dt.date.today().isoformat())
        json.dump(data, open("reenio_dump.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print("Uloženo do reenio_dump.json")
    elif a.cli:
        collect()
        for s in current_slots():
            if s["free"] is True:
                p = f'{s["price"]} Kč/{s["block"]} min' if s.get("price") else ""
                print(f'{s["start"]}–{s["end"][11:]}  {s["venue"]}  {s["court"]}  {p}')
        for e in STATE["errors"]: print("CHYBA:", e)
        print(f'Načteno za {STATE.get("duration")} s')
    else:
        srv = None
        for port in [a.port, 8765, 5000, 8000, 18080, 0]:
            try:
                srv = Server((a.host, port), H); break
            except OSError as ex:
                print(f"Port {port} nelze použít ({ex.__class__.__name__}), zkouším další…")
        if srv is None:
            raise SystemExit("Nepodařilo se otevřít žádný port.")
        threading.Thread(target=refresher, daemon=True).start()
        print(f"Běží na http://localhost:{srv.server_address[1]}  (ukončení Ctrl+C)")
        if a.host == "0.0.0.0":
            import socket
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]; s.close()
                print(f"Z mobilu (stejná Wi-Fi): http://{ip}:{srv.server_address[1]}")
            except OSError:
                print("Z mobilu: http://<IP-adresa-PC>:" + str(srv.server_address[1]))
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print("Ukončeno.")
