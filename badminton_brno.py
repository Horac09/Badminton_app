#!/usr/bin/env python3
"""
Badminton Brno - přehled volných kurtů
  * TJ Tesla Brno – Lesná (Reenio)
  * Xarena Slatina (Bizzy / e-rezervace.cz)
  * Centrum Viktoria (Clubspire timeline)
  * Ivanovice (Memberzone – rozpis)
  * Hala Sprint (Memberzone – Sportoviste.aspx)

Spuštění:
    pip install requests tzdata
    python badminton_brno.py                             # web (http://localhost:8765)
    python badminton_brno.py --host 0.0.0.0              # web dostupný i z mobilu na stejné Wi-Fi
    python badminton_brno.py --cli                       # výpis volných slotů do konzole
    python badminton_brno.py --export site               # statický web pro mobil (GitHub Pages)
    python badminton_brno.py --dump-xarena 25.11.2026    # ladění jednotlivých hal
    python badminton_brno.py --dump-viktoria 25.11.2026
    python badminton_brno.py --dump-memberzone 01.12.2026
    python badminton_brno.py --dump-sprint

JAK PŘIDAT DALŠÍ HALU / KURTY (viz KONFIGURACE níže):
  * Reenio (jako TJ Tesla)        -> přidat kurt do VENUES[..]["resources"] nebo novou položku do VENUES
  * Memberzone Sportoviste.aspx   -> přidat položku do SPORTOVISTE (stačí URL stránky sportoviště)
  * jiný rezervační systém        -> potřeba nový adaptér (pošlete cURL)
  * u každé haly lze vyplnit "maps" = odkaz na Google Mapy (zobrazí se 📍 v záhlaví)
"""
import argparse, json, os, re, threading, time, html as htmlmod, datetime as dt
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
GRID_DAY_FROM = "07:00"   # mřížka vždy ukáže aspoň tento rozsah (i bez volných termínů)
GRID_DAY_TO = "22:00"

# --- Reenio
VENUES = [
    {
        "name": "TJ Tesla Brno – Lesná",
        "maps": "https://maps.app.goo.gl/5XapA5iQAs23Rf2D7",
        "base": "https://tjteslabrno.reenio.cz",
        "section": "/cs/view/lesna-f1/sportovni-hala-f290/badminton-f292",
        # ID kurtu (číslo za "-r" v URL) -> (název, poslední část URL kurtu)
        "resources": {
            105438: ("Badminton I", "badminton-i-sportovni-hala-badminton-hriste-i-r105438"),
            105439: ("Badminton II", "badminton-ii-sportovni-hala-badminton-hriste-ii-r105439"),
        },
    },
]

# --- Bizzy / e-rezervace
XARENA = {
    "name": "Xarena Slatina",
    "maps": "https://maps.app.goo.gl/ekHmvTmJoYa74KTz5",
    "url": "https://xarena.e-rezervace.cz/Branch/pages/Schedule.faces",
    "service_id": "40106",        # výběr služby badminton
    "days_ahead": 14,
    "parallel": 3,
    "cookie": "",
}

# --- Clubspire timeline
VIKTORIA = {
    "name": "Centrum Viktoria",
    "maps": "https://maps.app.goo.gl/JddTpoGSQcCJxsYu8",
    "base": "https://rezervace.centrumviktoria.cz:18443",
    "sport_index": 1,      # tc[1].s=true = badminton
    "sport_count": 5,
    "days_ahead": 14,
    "parallel": 1,         # server shazuje souběžná spojení
    "delay": 1.0,
    "enabled": True,       # False = nenačítat (např. na firemní VPN, která Viktorii blokuje)
}

# --- Memberzone rozpis (Ivanovice)
MEMBERZONE = {
    "name": "Ivanovice",
    "maps": "https://maps.app.goo.gl/wHpfAQUA6p9YnEr79",
    "url": "https://memberzone.cz/ivanovice/",
    "sport_name": "Badminton",
    "days_ahead": 14,
    "parallel": 3,
}

# --- Memberzone Sportoviste.aspx (týdenní přehled volných začátků) – lze přidat další kluby
# --- Fit4All (obyčejná stránka: /cs/online-rezervace?date=YYYY-MM-DD&activity=6)
FIT4ALL = {
    "name": "Fit4All",
    "maps": "https://maps.app.goo.gl/iT1ABDjMrc2Z9n3B6",
    "url": "https://www.fit4all.cz/cs/online-rezervace",
    "activity": "6",              # badminton
    "days_ahead": 14,
    "enabled": True,
    "parallel": 3,
}

SPORTOVISTE = [
    {
        "name": "Hala Sprint",
        "maps": "https://maps.app.goo.gl/NgLHT3WhJdYyYNEb8",
        "url": "https://memberzone.cz/sprint_tenis/Sportoviste.aspx?ID_Sportoviste=3&NAZEV=Badminton+hala",
        "days_ahead": 14,
    },
]

# pořadí sloupců v mřížce + odkazy na mapy
_ALL_CFG = VENUES + [XARENA, VIKTORIA, MEMBERZONE] + SPORTOVISTE + [FIT4ALL]
VENUE_ORDER = [c["name"] for c in _ALL_CFG]
VENUE_MAPS = {c["name"]: c.get("maps", "") for c in _ALL_CFG}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36")

_host_locks = {}
def host_slot(host, n=PER_HOST):
    return _host_locks.setdefault(host, threading.BoundedSemaphore(n))

NET_ERRORS = (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError)

def _short(e):
    s = str(e)
    m = re.search(r"(\w+Error)\((\d+),\s*'([^']+)'", s)
    if m:
        hint = " – spojení blokuje síť (VPN/firewall?)" if m.group(2) == "10054" else ""
        return f"{m.group(1)} {m.group(2)}: {m.group(3)}{hint}"
    return (e.__class__.__name__ + ": " + s)[:200]

def fetch_days(make_client, fetch, days, label, delay=0.0):
    """Stáhne dny jedním klientem; při výpadku spojení opakuje, po MAX_FAILS_IN_ROW chybách skončí."""
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
                client = make_client()
                break
        fails_in_row = 0 if ok else fails_in_row + 1
        if fails_in_row >= MAX_FAILS_IN_ROW and i + 1 < len(days):
            errs.append(f"{label}: {MAX_FAILS_IN_ROW} dny za sebou selhaly – zbylých "
                        f"{len(days) - i - 1} dní v tomto kole přeskočeno")
            break
        if delay:
            time.sleep(delay)
    return out, errs

def form_fields(html):
    """Všechna pole ASP.NET formuláře: <input> (kromě tlačítek) + <select> (vybraná hodnota)."""
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
    for m in re.finditer(r'<select\b[^>]*name="([^"]+)"[^>]*>(.*?)</select>', html, re.S | re.I):
        opts = re.findall(r'<option\b([^>]*)value="([^"]*)"', m.group(2))
        sel = [v for a, v in opts if "selected" in a] or [v for a, v in opts[:1]]
        if sel:
            out[htmlmod.unescape(m.group(1))] = htmlmod.unescape(sel[0])
    return out

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

# ---------------------------------------------------------------- ADAPTÉR CENTRUM VIKTORIA
class ViktoriaClient:
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

# ---------------------------------------------------------------- ADAPTÉR MEMBERZONE ROZPIS (Ivanovice)
class MemberzoneClient:
    """GET -> postback tlačítka sportu -> postback datepickeru. Datum v kalendáři MM/dd/yyyy s nulami."""
    DP = "ASPxFormLayout2$Datepicker2"
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

    def _post(self, extra):
        f = form_fields(self.html)
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
        for pat in (r"<title>\s*(.*?)\s*</title>", r"<h2>\s*<i>(.*?)</i>"):
            m = re.search(pat, html or "", re.S | re.I)
            if m and m.group(1).strip():
                return re.sub(r"<[^>]+>|\s+", " ", m.group(1)).strip()[:200]
        return ""

    def _form(self, date, variant):
        q = "&quot;"
        prev = self.shown_date(self.html) or dt.date.today()
        vis = prev if variant.startswith("prev") else date
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
        for variant in order:
            try:
                html = self._post(self._form(date, variant))
            except requests.HTTPError as e:
                code = e.response.status_code if e.response is not None else "?"
                self.tried.append(f"{variant} -> HTTP {code}")
                self._new_session(); self._init_session()
                continue
            got = self.shown_date(html)
            if got and got != date:
                self.tried.append(f"{variant} -> den {got:%d.%m.}")
                continue
            self.variant = variant
            return html
        detail = self.error_title(self.last_error_html)
        raise RuntimeError(f"Memberzone neukázal {date:%d.%m.%Y} (zkoušeno: {'; '.join(self.tried)})"
                           + (f" – server: {detail}" if detail else ""))

# ---------------------------------------------------------------- ADAPTÉR MEMBERZONE SPORTOVISTE (Hala Sprint)
class SportovisteClient:
    """
    Týdenní stránka Sportoviste.aspx. Pro každý volný začátek blok skrytých polí:
      ctlNN$hfDatumCas = "20. 11. 2026 7:00:00", ctlNN$hfDelka = "60", ctlNN$hfKurtyName = "kurt č.3,kurt č.4"
    Další týden = postback __EVENTTARGET=btnNext.
    """
    def __init__(self, cfg):
        self.c = cfg
        self.s = requests.Session()
        self.s.headers.update({"user-agent": UA, "accept-language": "cs,en;q=0.9"})
        self.pages = []

    @staticmethod
    def shown_date(html):
        m = re.search(r'name="myDate"[^>]*value="(\d{4}-\d\d-\d\d)"', html)
        return dt.date.fromisoformat(m.group(1)) if m else None

    def _post(self, html, target, my_date=None):
        f = form_fields(html)
        f.update({"__EVENTTARGET": target, "__EVENTARGUMENT": ""})
        if my_date:
            f["myDate"] = my_date.isoformat()
        r = self.s.post(self.c["url"], data=f, timeout=30, headers={
            "origin": "https://memberzone.cz", "referer": self.c["url"],
            "content-type": "application/x-www-form-urlencoded"})
        r.raise_for_status()
        return r.text

    def fetch_all(self, until):
        """
        Stránka ukazuje cca týden od data v poli myDate. Tlačítko btnNext posouvá jen o 1 den,
        proto skáčeme přímo: nastavíme myDate na první dosud nepokrytý den a pošleme
        postback 'btnHidePom' (to samé dělá změna data v kalendáři).
        """
        r = self.s.get(self.c["url"], timeout=30)
        r.raise_for_status()
        html = r.text
        self.pages = [html]
        covered = dt.date.today() - dt.timedelta(days=1)
        for _ in range(8):
            start = self.shown_date(html) or covered
            days = [dt.date.fromisoformat(s["start"][:10]) for s in parse_sportoviste(html, dt.datetime(2000, 1, 1))]
            # stránka pokrývá aspoň do posledního dne s volnem; když je prázdná, posuneme o týden
            last = max(days) if days else start + dt.timedelta(days=6)
            covered = max(covered, last, start)
            if covered >= until:
                break
            nxt = covered + dt.timedelta(days=1)
            html = self._post(html, "btnHidePom", nxt)
            if self.shown_date(html) != nxt:          # skok nefunguje -> záloha po dnech
                html = self._post(html, "btnNext")
            self.pages.append(html)
        return self.pages

# ---------------------------------------------------------------- ADAPTÉR FIT4ALL
class Fit4AllClient:
    """Jeden GET na den. Odkaz na rezervaci = stejná adresa (otevře přímo daný den)."""
    def __init__(self, cfg=FIT4ALL):
        self.c = cfg
        self.s = requests.Session()
        self.s.headers.update({"user-agent": UA, "accept-language": "cs,en;q=0.9"})

    def day_url(self, date):
        return f'{self.c["url"]}?date={date.isoformat()}&activity={self.c["activity"]}'

    def fetch(self, date):
        r = self.s.get(self.day_url(date), timeout=20)
        r.raise_for_status()
        return r.text

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

# ---------------------------------------------------------------- PARSER Memberzone rozpis (Ivanovice)
_MZ_ROW = re.compile(r'<tr id="[^"]*RADEK_DAY_\d+"[^>]*>\s*<td[^>]*class="color_tmava_bcg">([^<]+)</td>(.*?)</tr>', re.S)
_MZ_CELL = re.compile(r'<td id="[^"]*_(\d{3})(\d{2})(\d{2})" colspan="(\d+)">\s*<input\b([^>]*)>', re.S)

def parse_memberzone(html, now=None, day=None, first_hour=6):
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

# ---------------------------------------------------------------- PARSER Memberzone Sportoviste (Hala Sprint)
_SP_FIELD = re.compile(r'<input\b[^>]*name="(ctl\d+)\$(hfDatumCas|hfDelka|hfKurtyName)"[^>]*>', re.I)

def parse_sportoviste(html, now=None):
    now = (now or dt.datetime.now()).replace(second=0, microsecond=0)
    blocks = {}
    for m in _SP_FIELD.finditer(html):
        v = re.search(r'\bvalue="([^"]*)"', m.group(0))
        blocks.setdefault(m.group(1), {})[m.group(2)] = htmlmod.unescape(v.group(1)) if v else ""
    free = {}
    for b in blocks.values():
        m = re.match(r"\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\s+(\d{1,2}):(\d{2})", b.get("hfDatumCas", ""))
        if not m:
            continue
        t0 = dt.datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), int(m.group(4)), int(m.group(5)))
        t1 = t0 + dt.timedelta(minutes=int(b.get("hfDelka") or 60))
        for court in filter(None, (c.strip() for c in b.get("hfKurtyName", "").split(","))):
            free.setdefault(court, []).append((t0, t1))
    out, step = [], dt.timedelta(minutes=30)
    for court, iv in free.items():
        name = re.sub(r"(?i)badminton\s+kurt\s*č\.?\s*", "Kurt ", court)
        for a, b in _merge(iv):
            t = a
            while t + step <= b:
                if t >= now:
                    out.append({"court": name, "start": t.strftime("%Y-%m-%d %H:%M"),
                                "end": (t + step).strftime("%Y-%m-%d %H:%M"),
                                "free": True, "opens": None, "price": None, "block": 30})
                t += step
    return out

# ---------------------------------------------------------------- PARSER Fit4All
_F4_CELL = re.compile(r'<div\b([^>]*class="court-cell[^"]*"[^>]*)>', re.S)

def parse_fit4all(html, now=None):
    """
    Každý 30min slot = <div data-name="Badminton KURT 1" data-date="25.11.2026" data-start-time="7:00"
    class="court-cell court-cell-free|court-cell-booked ...">.
    """
    now = (now or dt.datetime.now()).replace(second=0, microsecond=0)
    out = []
    for m in _F4_CELL.finditer(html):
        a = m.group(1)
        g = lambda k: (re.search(rf'{k}="([^"]*)"', a) or [None, ""])[1]
        d, t, name, cls = g("data-date"), g("data-start-time"), g("data-name"), g("class")
        dm, tm = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", d), re.match(r"(\d{1,2}):(\d{2})", t)
        if not (dm and tm):
            continue
        b0 = dt.datetime(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)), int(tm.group(1)), int(tm.group(2)))
        if b0 < now:
            continue
        court = re.sub(r"(?i)badminton\s+kurt\s*", "Kurt ", name).strip()
        out.append({"court": court, "start": b0.strftime("%Y-%m-%d %H:%M"),
                    "end": (b0 + dt.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M"),
                    "free": "court-cell-free" in cls, "opens": None, "price": None, "block": 30})
    return out

# ---------------------------------------------------------------- NAČÍTÁNÍ
STATE = {"results": {}, "updated": None, "errors": [], "done": 0, "total": 0, "loading": False}
_lock = threading.Lock()

POST_FORMS = {}          # id -> (url, {pole: hodnota}) – formuláře pro "otevřít přímo daný den"

def _register_form(fid, url, fields):
    with _lock:
        POST_FORMS[fid] = (url, fields)
    return fid

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

def viktoria_day_link(ds):
    """Stejná GET adresa, jakou posílá formulář Viktorie (sport + den)."""
    p = []
    for i in range(VIKTORIA["sport_count"]):
        if i == VIKTORIA["sport_index"]:
            p.append(f"tc%5B{i}%5D.s=true")
        p.append(f"_tc%5B{i}%5D.s=on")
    ts = ViktoriaClient._ts(dt.date.fromisoformat(ds))
    return f'{VIKTORIA["base"]}/timeline/day?{"&".join(p)}&criteriaTimestamp={ts}'

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

def _sportoviste_task(cfg):
    def run():
        until = dt.date.today() + dt.timedelta(days=cfg.get("days_ahead", DAYS_AHEAD))
        with host_slot("memberzone", 3):
            pages = None
            for attempt in range(1, RETRIES + 1):
                try:
                    pages = SportovisteClient(cfg).fetch_all(until)
                    break
                except NET_ERRORS:
                    if attempt == RETRIES:
                        raise
                    time.sleep(2.0 * attempt)
        def view(now):
            seen, res = set(), []
            for html in pages:
                for t in parse_sportoviste(html, now):
                    k = (t["court"], t["start"])
                    if k not in seen and t["start"][:10] <= until.isoformat():
                        seen.add(k)
                        res.append({**t, "venue": cfg["name"], "link": cfg["url"]})
            return res
        return view
    return (f'sportoviste|{cfg["name"]}', run)

def _fit4all_task(days):
    def run():
        with host_slot("fit4all", FIT4ALL["parallel"]):
            pages, errs = fetch_days(Fit4AllClient, lambda c, d: (d, c.fetch(d)), days, FIT4ALL["name"])
        _report(errs)
        fc = Fit4AllClient()
        def view(now):
            return [{**t, "venue": FIT4ALL["name"], "link": fc.day_url(d)}
                    for d, html in pages for t in parse_fit4all(html, now)]
        return view
    return (f'fit4all|{days[0]}', run)

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
    for cfg in SPORTOVISTE:
        tasks.append(_sportoviste_task(cfg))
    if FIT4ALL.get("enabled", True):
        for ch in _chunks(_days(FIT4ALL["days_ahead"]), FIT4ALL["parallel"]):
            tasks.append(_fit4all_task(ch))
    return tasks

_collect_lock = threading.Lock()

def collect():
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
                    STATE["errors"].append(f"{key}: {_short(e)}")
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
    # celý rozsah dne: od nejdřívějšího do nejpozdějšího času ze všech hal (i obsazené),
    # aby byly vidět i řádky bez volna. Záloha GRID_DAY_FROM–GRID_DAY_TO.
    day_slots = [s for s in slots if s["start"].startswith(day)]
    t0 = min([s["start"][11:] for s in day_slots] + [GRID_DAY_FROM])
    t1 = max([s["end"][11:] for s in day_slots] + [GRID_DAY_TO])
    t = dt.datetime.strptime(t0, "%H:%M")
    t = t.replace(minute=(t.minute // GRID_STEP_MIN) * GRID_STEP_MIN)
    end = dt.datetime.strptime(t1, "%H:%M")
    times = set(cells)
    while t < end:
        times.add(t.strftime("%H:%M"))
        t += step
    return sorted(times), cells

def _court_key(c):
    return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", c)]

def _plural(n):
    return "volný" if n == 1 else ("volné" if 2 <= n <= 4 else "volných")

_USED_FORMS = set()

OPEN_DAY_JS = """<script>
function openDay(id){var f=document.getElementById('f-'+id);if(!f)return true;f.submit();return false;}
</script>"""

def forms_html(ids):
    """Skryté formuláře, které otevřou rezervační web rovnou na daném dni (POST do nového okna)."""
    out = []
    with _lock:
        items = [(i, POST_FORMS[i]) for i in sorted(ids) if i in POST_FORMS]
    for fid, (url, fields) in items:
        inputs = "".join(f'<input type=hidden name="{htmlmod.escape(k)}" value="{htmlmod.escape(v)}">'
                         for k, v in fields.items())
        out.append(f'<form id="f-{fid}" method=post action="{htmlmod.escape(url)}" target=_blank '
                   f'accept-charset="utf-8" hidden>{inputs}</form>')
    return "".join(out)

def _cell_html(lst):
    """Vrací (css třída, vnitřek). Barva podle počtu volných kurtů: l1 / l2 / l3 (3+)."""
    if not lst:
        return "none", ""
    free_now = [s for s in lst if s["free"] is True]
    later = [s for s in lst if s["free"] is None]
    n = len({s["court"] for s in free_now})
    nl = len({s["court"] for s in later})
    cls = "l3" if n >= 3 else ("l2" if n == 2 else ("l1" if n == 1 else "later"))
    if n:
        label = f'<b>{n}</b><span>{_plural(n)}</span>'
        if nl:
            label += f'<em>+{nl} později</em>'
    else:
        label = f'<b>{nl}</b><span>později</span>'
    items = []
    for s in sorted(lst, key=lambda s: _court_key(s["court"])):
        meta = f'{s["start"][11:]}–{s["end"][11:]}'
        if s.get("price"):
            meta += f' · {s["price"]} Kč/{s["block"]} min'
        if not s["free"]:
            meta += f' · od {s["opens"]}'
        onclick = ""
        if s.get("post"):
            _USED_FORMS.add(s["post"])
            onclick = f' onclick="return openDay(\'{s["post"]}\')"'
        items.append(f'<a class="court{"" if s["free"] else " dim"}" href="{htmlmod.escape(s["link"])}" '
                     f'target=_blank rel=noopener{onclick}><span class=cn>{htmlmod.escape(s["court"])}</span>'
                     f'<span class=cm>{meta}</span><span class=go>↗</span></a>')
    return cls, f'<details><summary>{label}</summary><div class=courts>{"".join(items)}</div></details>'

def _venue_head(v):
    maps = VENUE_MAPS.get(v)
    pin = (f'<a class=pin href="{htmlmod.escape(maps)}" target=_blank rel=noopener title="Otevřít v Google Mapách">'
           f'📍</a>') if maps else ""
    return f'<th><div class=vh><span>{htmlmod.escape(v)}</span>{pin}</div></th>'

def render_grid(slots, day, show_later):
    times, cells = build_grid(slots, day, show_later)
    venues = list(VENUE_ORDER)
    if not times:
        return "<div class=empty>V tento den není nic volného 😕</div>"
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
    head = "<tr><th class=tcol></th>" + "".join(_venue_head(v) for v in venues) + "</tr>"
    rows = []
    for tm in times:
        full = tm.endswith(":00")
        tds = [f'<td class="tcol{" full" if full else ""}">{tm}</td>']
        for v in venues:
            sp = span.get((tm, v), 1)
            if sp == 0:
                continue
            cls, inner = _cell_html(cells.get(tm, {}).get(v, []))
            rs = " rowspan=2" if sp == 2 else ""
            inner = f'<div class="blk {cls}{" hr" if sp == 2 else ""}">{inner}</div>' if inner else ""
            tds.append(f'<td{rs}>{inner}</td>')
        rows.append(f'<tr data-t="{tm}"{" class=full" if full else ""}>' + "".join(tds) + "</tr>")
    return f"<div class=gridwrap><table class=grid>{head}{''.join(rows)}</table></div>"

def render_list(slots, day, show_later, show_all):
    rows = []
    for s in slots:
        if day and not s["start"].startswith(day): continue
        if s["free"] is False and not show_all: continue
        if s["free"] is None and not (show_later or show_all): continue
        if s["free"] is True:   cls, txt = "st-free", "volno"
        elif s["free"] is None: cls, txt = "st-later", f'od {s["opens"]}'
        else:                   cls, txt = "st-busy", "obsazeno"
        price = f'{s["price"]} Kč / {s.get("block", 30)} min' if s.get("price") else ""
        oc = ""
        if s.get("post"):
            _USED_FORMS.add(s["post"]); oc = f' onclick="return openDay(\'{s["post"]}\')"'
        rows.append(f'<a class="row" href="{htmlmod.escape(s["link"])}" target=_blank rel=noopener{oc}>'
                    f'<span class=rt>{s["start"][11:]}–{s["end"][11:]}</span>'
                    f'<span class=rv>{htmlmod.escape(s["venue"])}<small>{htmlmod.escape(s["court"])}</small></span>'
                    f'<span class=rp>{price}</span><span class="st {cls}">{txt}</span></a>')
    return f'<div class=list>{"".join(rows) or "<div class=empty>Nic nenalezeno</div>"}</div>'

# ---------------------------------------------------------------- VZHLED (světlý / tmavý podle systému)
CSS = """
:root{
  --bg:#f3f5f7;--card:#ffffff;--text:#18212b;--muted:#6b7785;--line:#e3e8ee;
  --accent:#16a34a;--accent-ink:#ffffff;
  --g1:#dcfce7;--g1t:#166534;--g2:#86efac;--g2t:#14532d;--g3:#22c55e;--g3t:#ffffff;
  --later:#eef1f4;--latert:#6b7785;--err:#dc2626;--shadow:0 1px 2px rgba(16,24,40,.06),0 1px 3px rgba(16,24,40,.08);
}
:root[data-theme=dark]{
  --bg:#0f1418;--card:#171e24;--text:#e6edf3;--muted:#8b98a5;--line:#26313a;
  --accent:#22c55e;--accent-ink:#06210f;
  --g1:#14351f;--g1t:#86efac;--g2:#166534;--g2t:#dcfce7;--g3:#22c55e;--g3t:#052e14;
  --later:#1e262d;--latert:#8b98a5;--err:#f87171;--shadow:0 1px 2px rgba(0,0,0,.4);
}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){
  --bg:#0f1418;--card:#171e24;--text:#e6edf3;--muted:#8b98a5;--line:#26313a;
  --accent:#22c55e;--accent-ink:#06210f;
  --g1:#14351f;--g1t:#86efac;--g2:#166534;--g2t:#dcfce7;--g3:#22c55e;--g3t:#052e14;
  --later:#1e262d;--latert:#8b98a5;--err:#f87171;--shadow:0 1px 2px rgba(0,0,0,.4);
}}
*{box-sizing:border-box}
body{margin:0;font:15px/1.4 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);
  -webkit-tap-highlight-color:transparent}
a{color:inherit}
header{position:sticky;top:0;z-index:5;background:var(--bg);padding:14px 16px 8px;border-bottom:1px solid var(--line)}
.top{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
h1{font-size:20px;margin:0;font-weight:700;letter-spacing:-.01em;flex:1}
.upd{color:var(--muted);font-size:13px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;border-radius:999px;background:var(--accent);
  color:var(--accent-ink);text-decoration:none;font-weight:600;font-size:14px;border:0;cursor:pointer}
.btn.warn{background:var(--err);color:#fff}
.theme{display:inline-flex;background:var(--card);border-radius:999px;box-shadow:var(--shadow);padding:3px}
.theme button{border:0;background:none;color:var(--muted);font:inherit;font-size:13px;padding:4px 10px;border-radius:999px;cursor:pointer}
.theme button.on{background:var(--accent);color:var(--accent-ink);font-weight:600}
.btn.ghost{background:var(--card);color:var(--text);box-shadow:var(--shadow)}
.days{display:flex;gap:6px;overflow-x:auto;padding:10px 0 4px;scrollbar-width:none}
.days::-webkit-scrollbar{display:none}
.days a{flex:0 0 auto;display:flex;flex-direction:column;align-items:center;min-width:52px;padding:6px 8px;border-radius:12px;
  background:var(--card);box-shadow:var(--shadow);text-decoration:none;color:var(--text);font-size:12px}
.days a b{font-size:16px}
.days a.sel{background:var(--accent);color:var(--accent-ink)}
.days a.we:not(.sel) b{color:var(--accent)}
.bar{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.bar a{font-size:13px;padding:5px 12px;border-radius:999px;background:var(--card);box-shadow:var(--shadow);text-decoration:none}
.bar a.on{background:var(--accent);color:var(--accent-ink)}
.legend{display:flex;gap:10px;align-items:center;color:var(--muted);font-size:12px;margin-top:8px;flex-wrap:wrap}
.legend i{display:inline-block;width:14px;height:14px;border-radius:4px;vertical-align:-2px;margin-right:4px}
main{padding:12px 16px 40px}
.gridwrap{overflow-x:auto;border-radius:16px;background:var(--card);box-shadow:var(--shadow)}
table.grid{border-collapse:separate;border-spacing:0;width:100%;min-width:640px}
table.grid th{position:sticky;top:0;background:var(--card);z-index:2;padding:12px 6px 8px;font-size:13px;font-weight:600;
  text-align:left;border-bottom:1px solid var(--line)}
.vh{display:flex;align-items:center;gap:6px;justify-content:space-between}
.pin{text-decoration:none;font-size:15px;padding:2px 6px;border-radius:8px;background:var(--later)}
table.grid td{padding:2px 3px;vertical-align:top}
tr.full td{border-top:1px solid var(--line)}
td.tcol{position:sticky;left:0;background:var(--card);z-index:1;width:54px;color:var(--muted);font-size:12px;
  font-variant-numeric:tabular-nums;padding:6px 8px 0 12px}
td.tcol.full{color:var(--text);font-weight:600}
.blk{border-radius:10px;height:100%;min-height:38px;transition:transform .1s}
.blk.hr{min-height:80px;display:flex;align-items:center}
.blk.hr details{width:100%}
.blk.l1{background:var(--g1);color:var(--g1t)}
.blk.l2{background:var(--g2);color:var(--g2t)}
.blk.l3{background:var(--g3);color:var(--g3t)}
.blk.later{background:var(--later);color:var(--latert)}
summary{list-style:none;cursor:pointer;padding:8px 10px;display:flex;align-items:baseline;gap:5px;flex-wrap:wrap}
summary::-webkit-details-marker{display:none}
summary b{font-size:17px;line-height:1}
summary span{font-size:12px;opacity:.85}
summary em{font-size:11px;font-style:normal;opacity:.75}
details[open]{background:var(--card);color:var(--text);border-radius:10px;box-shadow:0 6px 20px rgba(0,0,0,.18);
  position:relative;z-index:3}
details[open] summary{border-bottom:1px solid var(--line)}
.courts{display:flex;flex-direction:column;padding:6px}
.court{display:flex;align-items:center;gap:8px;padding:8px;border-radius:8px;text-decoration:none}
.court:hover{background:var(--later)}
.court.dim{opacity:.6}
.cn{font-weight:600;font-size:14px}
.cm{color:var(--muted);font-size:12px;flex:1}
.go{color:var(--accent);font-weight:700}
.list{display:flex;flex-direction:column;gap:6px}
.row{display:grid;grid-template-columns:96px 1fr auto auto;gap:10px;align-items:center;padding:10px 14px;border-radius:12px;
  background:var(--card);box-shadow:var(--shadow);text-decoration:none}
.rt{font-weight:600;font-variant-numeric:tabular-nums}
.rv small{display:block;color:var(--muted);font-size:12px}
.rp{color:var(--muted);font-size:12px}
.st{font-size:12px;padding:3px 10px;border-radius:999px;font-weight:600}
.st-free{background:var(--g3);color:var(--g3t)}.st-later{background:var(--later);color:var(--latert)}
.st-busy{background:var(--later);color:var(--muted);text-decoration:line-through}
.empty{padding:40px;text-align:center;color:var(--muted);background:var(--card);border-radius:16px;box-shadow:var(--shadow)}
.errs{margin-top:14px;color:var(--err);font-size:12px}
.errs summary{color:var(--err);padding:4px 0}
.load{color:#d97706;font-weight:600}
@media (max-width:700px){
  header{padding:10px 10px 6px}main{padding:8px 6px 40px}h1{font-size:18px}
  table.grid th{font-size:11px}td.tcol{width:44px;padding-left:8px}
  .row{grid-template-columns:80px 1fr auto}.rp{display:none}
}
"""

PAGE = """<!doctype html><html lang=cs><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<meta name=color-scheme content="light dark">
<meta name=theme-color media="(prefers-color-scheme: light)" content="#f3f5f7">
<meta name=theme-color media="(prefers-color-scheme: dark)" content="#0f1418">
%(head)s<title>Badminton Brno</title><style>%(css)s</style>
<script>(function(){var t=localStorage.getItem('theme')||'system';
if(t!=='system')document.documentElement.dataset.theme=t;})();
function setTheme(t){localStorage.setItem('theme',t);
 if(t==='system')delete document.documentElement.dataset.theme;else document.documentElement.dataset.theme=t;
 document.querySelectorAll('.theme button').forEach(b=>b.classList.toggle('on',b.dataset.t===t));}
document.addEventListener('DOMContentLoaded',()=>setTheme(localStorage.getItem('theme')||'system'));</script>
<header>
  <div class=top><h1>🏸 Volné kurty · Brno</h1><span class=upd>%(status)s</span>%(button)s
  <span class=theme><button data-t=light onclick="setTheme('light')">☀ Světlý</button><button data-t=dark onclick="setTheme('dark')">☾ Tmavý</button><button data-t=system class=on onclick="setTheme('system')">Systém</button></span></div>
  <div class=days>%(days)s</div>
  %(bar)s
  <div class=legend><span><i style="background:var(--g1)"></i>1 kurt</span><span><i style="background:var(--g2)"></i>2 kurty</span>
  <span><i style="background:var(--g3)"></i>3 a více</span><span>📍 = mapa</span></div>
</header>
<main>%(body)s%(errs)s</main>
%(script)s</html>"""

def _day_tab(d, sel, href, extra=""):
    we = " we" if d.weekday() >= 5 else ""
    return (f'<a class="{"sel" if sel else ""}{we}" href="{href}" data-d="{d.isoformat()}"{extra}>'
            f'{DOW[d.weekday()]}<b>{d.day}</b></a>')

def _errs_html(errs):
    if not errs:
        return ""
    return (f'<details class=errs><summary>⚠ {len(errs)} chyb při načítání</summary>'
            + "".join(f"<div>{htmlmod.escape(e)}</div>" for e in errs) + "</details>")

def _max_days():
    return max([DAYS_AHEAD, XARENA["days_ahead"] + 1, VIKTORIA["days_ahead"] + 1, MEMBERZONE["days_ahead"] + 1]
               + [s.get("days_ahead", DAYS_AHEAD) + 1 for s in SPORTOVISTE])

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
        days = "".join(_day_tab(today + dt.timedelta(days=i), (today + dt.timedelta(days=i)).isoformat() == day,
                                link(d=(today + dt.timedelta(days=i)).isoformat()))
                       for i in range(_max_days()))
        bar = [f'<a href="{link(v="list" if view == "grid" else "grid")}">'
               f'{"☰ Seznam" if view == "grid" else "▦ Mřížka"}</a>',
               f'<a class="{"on" if show_later else ""}" href="{link(later=None if show_later else 1)}">'
               f'Ukázat i neotevřené rezervace</a>']
        if view == "list":
            bar.append(f'<a class="{"on" if show_all else ""}" href="{link(all=None if show_all else 1)}">i obsazené</a>')
        slots = current_slots()
        _USED_FORMS.clear()
        body = render_grid(slots, day, show_later) if view == "grid" else render_list(slots, day, show_later, show_all)
        body += forms_html(_USED_FORMS) + OPEN_DAY_JS
        with _lock:
            loading, done, total = STATE["loading"], STATE["done"], STATE["total"]
            upd, dur, errs = STATE["updated"], STATE.get("duration"), list(STATE["errors"])
        if loading:
            status = f'<span class=load>Načítám… {done}/{total}</span>'
            button = ""
        else:
            status = f'Aktualizováno {upd or "–"}' + (f' · {dur} s' if dur else "")
            button = f'<a class="btn{" warn" if errs else ""}" href="/refresh?{urlencode(params)}">🔄 Obnovit</a>'
        page = PAGE % {"head": '<meta http-equiv=refresh content=3>' if loading else "", "css": CSS,
                       "status": status, "button": button, "days": days,
                       "bar": f'<div class=bar>{"".join(bar)}</div>', "body": body,
                       "errs": _errs_html(errs), "script": ""}
        self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers(); self.wfile.write(page.encode())

class Server(ThreadingMixIn, HTTPServer):
    daemon_threads = True

# ---------------------------------------------------------------- EXPORT (statická PWA pro mobil)
def _icon_png(path, size):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return False
    img = Image.new("RGB", (size, size), (22, 163, 74))
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", int(size * 0.42))
    except OSError:
        font = ImageFont.load_default()
    box = d.textbbox((0, 0), "BB", font=font)
    d.text(((size - (box[2] - box[0])) / 2 - box[0], (size - (box[3] - box[1])) / 2 - box[1]),
           "BB", fill="white", font=font)
    img.save(path)
    return True

EXPORT_JS = """<script>
function show(d){
  document.querySelectorAll('.day').forEach(e=>e.hidden=e.id!=='d'+d);
  document.querySelectorAll('.days a').forEach(a=>a.classList.toggle('sel',a.dataset.d===d));
  history.replaceState(null,'','#'+d);
}
function toggleLater(a){
  document.body.classList.toggle('showlater');a.classList.toggle('on');
}
function hidePast(){
  const now=new Date(),p=n=>String(n).padStart(2,'0');
  const today=now.getFullYear()+'-'+p(now.getMonth()+1)+'-'+p(now.getDate());
  const hm=p(now.getHours())+':'+p(now.getMinutes());
  const el=document.getElementById('d'+today);
  if(el) el.querySelectorAll('tr[data-t]').forEach(r=>{if(r.dataset.t<hm) r.hidden=true;});
}
document.addEventListener('click',e=>{   // otevřený blok zavřít kliknutím jinam
  document.querySelectorAll('details[open]').forEach(d=>{if(!d.contains(e.target)) d.open=false;});
});
document.addEventListener('DOMContentLoaded',()=>{
  const first=document.querySelector('.days a'),want=location.hash.slice(1);
  show(document.getElementById('d'+want)?want:first.dataset.d);
  hidePast();
  if('serviceWorker' in navigator) navigator.serviceWorker.register('sw.js');
});
</script>"""

EXPORT_SW = """
const C='badminton-v2';
self.addEventListener('install',e=>{self.skipWaiting();e.waitUntil(caches.open(C).then(c=>c.addAll(['./','index.html','manifest.webmanifest','icon-192.png','icon-512.png'])))});
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',e=>{
  e.respondWith(fetch(e.request).then(r=>{const k=r.clone();caches.open(C).then(c=>c.put(e.request,k));return r})
    .catch(()=>caches.match(e.request)));
});
"""

def export_site(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    collect()
    slots = current_slots()
    today = dt.date.today()
    tabs, days = [], []
    _USED_FORMS.clear()
    for i in range(_max_days()):
        d = today + dt.timedelta(days=i)
        ds = d.isoformat()
        tabs.append(_day_tab(d, False, f"#{ds}", f' onclick="show(\'{ds}\');return false"'))
        # obě varianty (jen volné / i neotevřené) – přepínač je jen CSS
        days.append(f'<div class=day id="d{ds}" hidden><div class=v-now>{render_grid(slots, ds, False)}</div>'
                    f'<div class=v-later>{render_grid(slots, ds, True)}</div></div>')
    with _lock:
        upd, errs = STATE["updated"], list(STATE["errors"])
    css = CSS + ".v-later{display:none}.showlater .v-later{display:block}.showlater .v-now{display:none}"
    head = ('<meta name=apple-mobile-web-app-capable content=yes>'
            '<meta name=apple-mobile-web-app-status-bar-style content=black-translucent>'
            '<meta name=apple-mobile-web-app-title content="Badminton">'
            '<link rel=manifest href="manifest.webmanifest"><link rel=apple-touch-icon href="icon-192.png">')
    html = PAGE % {"head": head, "css": css, "status": f"Aktualizováno {upd}",
                   "button": '<a class="btn ghost" href="#" onclick="location.reload();return false">🔄</a>',
                   "days": "".join(tabs),
                   "bar": '<div class=bar><a href="#" onclick="toggleLater(this);return false">'
                          'Ukázat i neotevřené rezervace</a></div>',
                   "body": "".join(days) + forms_html(_USED_FORMS), "errs": _errs_html(errs),
                   "script": EXPORT_JS + OPEN_DAY_JS}
    open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8").write(html)
    json.dump({"name": "Badminton Brno – volné kurty", "short_name": "Badminton",
               "start_url": "./", "display": "standalone", "background_color": "#0f1418",
               "theme_color": "#16a34a",
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
    ap.add_argument("--dump-xarena", nargs="?", const="+1", default=None)
    ap.add_argument("--dump-viktoria", nargs="?", const="+1", default=None)
    ap.add_argument("--dump-memberzone", nargs="?", const="+2", default=None)
    ap.add_argument("--dump-sprint", action="store_true")
    ap.add_argument("--dump-fit4all", nargs="?", const="+1", default=None)
    ap.add_argument("--cli", action="store_true")
    ap.add_argument("--export", metavar="SLOZKA")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    a = ap.parse_args()
    if a.export:
        n, errs = export_site(a.export)
        print(f"Export hotov: {a.export}/index.html ({n} slotů, {len(errs)} chyb)")
        for e in errs: print("CHYBA:", e)
    elif a.dump_fit4all:
        d = _parse_day(a.dump_fit4all)
        fc = Fit4AllClient()
        html = fc.fetch(d)
        open("fit4all_dump.html", "w", encoding="utf-8").write(html)
        sl = parse_fit4all(html, dt.datetime(2000, 1, 1))
        print(f"Staženo {fc.day_url(d)}: bloků {len(sl)}, volných {sum(s['free'] for s in sl)}")
    elif a.dump_sprint:
        cfg = SPORTOVISTE[0]
        pages = SportovisteClient(cfg).fetch_all(dt.date.today() + dt.timedelta(days=cfg["days_ahead"]))
        for i, h in enumerate(pages):
            open(f"sprint_dump_{i}.html", "w", encoding="utf-8").write(h)
            sl = parse_sportoviste(h, dt.datetime(2000, 1, 1))
            ds = sorted({s["start"][:10] for s in sl})
            print(f"Týden {i}: {len(sl)} volných 30min bloků, dny {ds[0] if ds else '–'} … {ds[-1] if ds else '–'}")
    elif a.dump_memberzone:
        d = _parse_day(a.dump_memberzone)
        mz = MemberzoneClient()
        html = ""
        try:
            html = mz.fetch(d)
            print(f"OK – funkční varianta: {mz.variant}")
        except Exception as e:
            print("POZOR:", e)
        if mz.last_error_html:
            open("memberzone_error.html", "w", encoding="utf-8").write(mz.last_error_html)
        open("memberzone_dump.html", "w", encoding="utf-8").write(html)
        sl = parse_memberzone(html, dt.datetime(2000, 1, 1), d) if html else []
        print(f"Zobrazený den {mz.shown_date(html) if html else '–'}; bloků: {len(sl)}, volných: {sum(s['free'] for s in sl)}")
    elif a.dump_viktoria:
        d = _parse_day(a.dump_viktoria)
        vk = ViktoriaClient()
        try:
            html = vk.fetch(d)
        except Exception as e:
            raise SystemExit(f"Chyba: {_short(e)}")
        open("viktoria_dump.html", "w", encoding="utf-8").write(html)
        print(f"CSRF nalezen: {bool(vk.csrf)}; bloků: {len(parse_viktoria(html, dt.datetime(2000, 1, 1)))}")
    elif a.dump_xarena:
        d = _parse_day(a.dump_xarena)
        x = XarenaClient()
        try:
            _, html = x.fetch(d)
        except RuntimeError as e:
            print("POZOR:", e); html = x.last_html
        open("xarena_dump.html", "w", encoding="utf-8").write(html)
        n = len(parse_xarena(html, dt.datetime(2000, 1, 1), d)) if html else 0
        print(f"Rozvrh v odpovědi: {x._has_schedule(html)}, bloků: {n}")
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
                print(f"Z mobilu (stejná Wi-Fi): http://{s.getsockname()[0]}:{srv.server_address[1]}"); s.close()
            except OSError:
                pass
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print("Ukončeno.")
