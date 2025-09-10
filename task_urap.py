# -*- coding: utf-8 -*-
import os, re, sys, io, json, hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np

AID_REQ_TIMEOUT = float(os.environ.get("AID_REQ_TIMEOUT", "7.0"))
AID_GOOGLE_TIMEOUT = float(os.environ.get("AID_GOOGLE_TIMEOUT", "5.0"))
AID_SCRAPE_LIMIT = int(os.environ.get("AID_SCRAPE_LIMIT", "8"))
AID_THREADS = int(os.environ.get("AID_THREADS", "8"))
AID_TOTAL_BUDGET_SEC = float(os.environ.get("AID_TOTAL_BUDGET_SEC", "90"))
AID_SKIP_PDF = os.environ.get("AID_SKIP_PDF", "1") == "1"

_SESSION = None
def _session():
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry = Retry(total=2, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504))
        s.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20))
        s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20))
        s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en;q=0.8"})
        _SESSION = s
    return _SESSION

try:
    import requests
    from bs4 import BeautifulSoup
    HAVE_WEB = True
except Exception:
    HAVE_WEB = False

try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except Exception:
    pdf_extract_text = None

try:
    from dateparser.search import search_dates
except Exception:
    search_dates = None

from urllib.parse import urlparse, parse_qs

try:
    # pip install googletrans==4.0.0rc1
    from googletrans import Translator
    _translator = Translator()
except Exception:
    _translator = None

def _tr(text: str, target_lang: Optional[str]) -> str:
    if not target_lang or target_lang.lower() in ("en","eng","english"): return text
    if _translator is None: return text
    try: return _translator.translate(text, dest=target_lang).text
    except Exception: return text

# ---------- misc helpers ----------
def _norm(h):
    return re.sub(r'[^a-z0-9]+','_', str(h).strip().lower())

def _pick(hdr_map: Dict[str,str], *alts, contains_ok=True) -> Optional[str]:
    for a in alts:
        k = _norm(a)
        if k in hdr_map: return hdr_map[k]
    if contains_ok and alts:
        tgt = _norm(alts[0])
        for k,v in hdr_map.items():
            if tgt and tgt in k: return v
    return None

def _safe_sheet_name(name: str) -> str:
    bad = set('[]:*?/\\')
    clean = ''.join('-' if ch in bad else ch for ch in str(name))
    clean = clean.strip() or 'Sheet'
    return clean[:31]

def read_kiel_main(xlsx_path: str) -> pd.DataFrame:
    return pd.read_excel(xlsx_path, sheet_name="Bilateral Assistance, MAIN DATA")

# ---------- country profiles ----------
# ---------- country profiles (BIG) ----------
GENERIC_NEWS = [
    "reuters.com","apnews.com","bbc.com","politico.eu","euractiv.com","ft.com","aljazeera.com"
]
GENERIC_GOV  = ["nato.int","europa.eu","eeas.europa.eu"]
EU_INTL_NEWS = ["consilium.europa.eu","ec.europa.eu","europa.eu","eeas.europa.eu","europarl.europa.eu"]
IFIS         = ["worldbank.org","ebrd.com","eib.org","coebank.org","imf.org"]
UA_GOV       = ["president.gov.ua","kmu.gov.ua","mfa.gov.ua","mod.gov.ua","me.gov.ua","minfin.gov.ua"]
SECURITY     = ["nato.int","osce.org","undp.org","unicef.org","who.int"]
OSINT        = ["sipri.org","oryxspioenkop.com","mil.in.ua","ukdefencejournal.org.uk"]

# ---------- FX & money parsing ----------
FX_CACHE: Dict[str, float] = {}

def _fx_rate_to_eur(ccy: str) -> float:
    c = (ccy or "").upper().strip()
    if not c or c == "EUR": 
        return 1.0
    if c in FX_CACHE:
        return FX_CACHE[c]
    try:
        if HAVE_WEB:
            r = requests.get(
                f"https://api.exchangerate.host/latest",
                params={"base": c, "symbols": "EUR"},
                timeout=10
            )
            if r.ok:
                rate = (r.json() or {}).get("rates", {}).get("EUR")
                if rate and rate > 0:
                    FX_CACHE[c] = float(rate)
                    return FX_CACHE[c]
    except Exception:
        pass
    FALLBACK = {
        "USD": 0.92, "GBP": 1.17, "CHF": 1.02, "CAD": 0.68, "AUD": 0.62, "NZD": 0.56,
        "SEK": 0.089, "NOK": 0.084, "DKK": 0.134, "PLN": 0.23, "CZK": 0.040, "HUF": 0.0026,
        "RON": 0.20, "BGN": 0.51, "TRY": 0.028, "ILS": 0.26, "INR": 0.011, "CNY": 0.13,
        "JPY": 0.0064, "KRW": 0.00068, "MXN": 0.052, "BRL": 0.18, "ZAR": 0.049,
        "SAR": 0.245, "AED": 0.25, "QAR": 0.25, "KWD": 3.0, "TWD": 0.029, "UAH": 0.024
    }
    FX_CACHE[c] = FALLBACK.get(c, 1.0)
    return FX_CACHE[c]

_MULT_MAP = {
    # en
    "billion":1e9,"bn":1e9,"b":1e9,"million":1e6,"mln":1e6,"m":1e6,"thousand":1e3,"k":1e3,
    # de/fr/it/es/pt/nl/pl/cz/sk/ro/hr/sl/hu/fi/sv/da/no
    "mrd":1e9,"mrd.":1e9,"milliard":1e9,"milliards":1e9,"miliardi":1e9,"miljard":1e9,"miljarder":1e9,
    "mio":1e6,"mio.":1e6,"millions":1e6,"milioni":1e6,"miljoner":1e6,"milj.":1e6,
    "mln.":1e6,"mld":1e9,"mld.":1e9,"tys.":1e3,
    # ru/uk/bg/ro etc.
    "млрд":1e9,"млн":1e6,"тыс":1e3,"тис.":1e3,
    # tr
    "milyar":1e9,"milyon":1e6,"bin":1e3,
    # zh/ko
    "億":1e8,"万":1e4,"萬":1e4,"兆":1e12,"억":1e8,"만":1e4,"조":1e12,
}

_CUR_TAGS = {
    "€":"EUR","eur":"EUR","euro":"EUR",
    "$":"USD","usd":"USD","us$":"USD",
    "£":"GBP","gbp":"GBP",
    "¥":"JPY","jpy":"JPY","円":"JPY",
    "₩":"KRW","krw":"KRW",
    "₺":"TRY","try":"TRY","tl":"TRY",
    "chf":"CHF","cad":"CAD","aud":"AUD","nzd":"NZD",
    "sek":"SEK","nok":"NOK","dkk":"DKK","czk":"CZK","huf":"HUF","pln":"PLN","zł":"PLN","zl":"PLN",
    "ron":"RON","bgn":"BGN","uah":"UAH","ils":"ILS","₪":"ILS","cny":"CNY","rmb":"CNY","₽":"RUB",
    "mxn":"MXN","brl":"BRL","zar":"ZAR","sar":"SAR","aed":"AED","qar":"QAR","kwd":"KWD",
    "nt$":"TWD","twd":"TWD","ntd":"TWD","元":"CNY","新台幣":"TWD","新臺幣":"TWD"
}

_WORD_CUR = {
    "dollar": "USD", "dollars": "USD",
    "euro": "EUR", "euros": "EUR",
    "pound": "GBP", "pounds": "GBP", "sterling": "GBP",
    "zloty": "PLN", "złoty": "PLN", "zlotych": "PLN",
    "koruna": "CZK", "krona": "SEK", "kronor": "SEK", "krone": "NOK",
    "shekel": "ILS", "shekels": "ILS",
    "hryvnia": "UAH", "franc": "CHF", "francs": "CHF",
}

def _norm_currency_tag(tok: str) -> Optional[str]:
    t = (tok or "").lower().strip()
    return _CUR_TAGS.get(t) or _WORD_CUR.get(t) or (t.upper() if len(t)==3 else None)

_MULT_ALTS = "|".join(
    sorted((re.escape(k) for k in _MULT_MAP.keys()), key=len, reverse=True)
)

_RX_BEFORE = re.compile(
    r'(?P<cur>€|\$|£|¥|₩|₺|₪|zł|nt\$)\s*'
    r'(?P<num>(?:\d{1,3}(?:[.,\s]\d{3})+|\d+)(?:[.,]\d+)?)'
    r'(?:\s?(?P<mult>(?:' + _MULT_ALTS + r')))?',
    re.IGNORECASE
)

_RX_AFTER = re.compile(
    r'(?P<num>(?:\d{1,3}(?:[.,\s]\d{3})+|\d+)(?:[.,]\d+)?)'
    r'\s?(?P<mult>(?:' + _MULT_ALTS + r'))?'
    r'\s?(?P<cur>[A-Za-z]{3}|€|\$|£|¥|₩|₺|₪|zł|nt\$|'
    r'euro|euros?|usd|us\$|dollars?|gbp|pounds?|sterling|'
    r'jpy|yen|krw|won|try|lira|pln|złoty|zloty|zlotych|twd|'
    r'chf|francs?|cad|aud|nzd|sek|krona|kronor|nok|krone|dkk|czk|koruna|'
    r'huf|ron|bgn|uah|hryvnia|ils|shekels?|cny|rmb|mxn|brl|zar|sar|aed|qar|kwd)',
    re.IGNORECASE
)

def _to_float(num: str) -> float:
    s = num.replace(" ", "")
    if s.count(",") > 0 and s.count(".") > 0:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return float(re.sub(r"[^\d\.]", "", s) or 0.0)

def _multiplier(tok: Optional[str]) -> float:
    if not tok: return 1.0
    t = tok.lower().strip().strip(".")
    return _MULT_MAP.get(t, 1.0)

def extract_money_eur(text: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if not text: 
        return (None, None, None)
    cands: List[Tuple[float,str,str]] = []

    for m in _RX_BEFORE.finditer(text):
        cur = _norm_currency_tag(m.group("cur"))
        num = _to_float(m.group("num") or "0")
        mul = _multiplier(m.group("mult"))
        if cur:
            eur = num * mul * _fx_rate_to_eur(cur)
            frag = m.group(0)
            cands.append((eur, cur, frag))

    for m in _RX_AFTER.finditer(text):
        cur = _norm_currency_tag(m.group("cur"))
        num = _to_float(m.group("num") or "0")
        mul = _multiplier(m.group("mult"))
        if cur:
            eur = num * mul * _fx_rate_to_eur(cur)
            frag = m.group(0)
            cands.append((eur, cur, frag))

    if not cands:
        return (None, None, None)

    cands.sort(key=lambda x: x[0], reverse=True)
    val_eur, cur, frag = cands[0]
    if val_eur <= 0:
        return (None, cur, frag)
    return (float(val_eur), cur, frag)

COUNTRY_PROFILES: Dict[str, Dict] = {
    "united states": {
        "lang":"en",
        "news":["reuters.com","apnews.com","nytimes.com","washingtonpost.com","wsj.com","politico.com"],
        "gov":["defense.gov","state.gov","whitehouse.gov","treasury.gov","usaid.gov"]
    },
    "united kingdom": {
        "lang":"en",
        "news":["bbc.co.uk","theguardian.com","ft.com","telegraph.co.uk","times.co.uk","independent.co.uk","sky.com"],
        "gov":["gov.uk","parliament.uk","army.mod.uk","raf.mod.uk","royalnavy.mod.uk"]
    },
    "canada": {
        "lang":"en",
        "news":["cbc.ca","ctvnews.ca","globalnews.ca","theglobeandmail.com","nationalpost.com"],
        "gov":["canada.ca","forces.gc.ca","international.gc.ca","pm.gc.ca","fin.gc.ca"]
    },
    "australia": {
        "lang":"en",
        "news":["abc.net.au","smh.com.au","theage.com.au","theaustralian.com.au","news.com.au","sbs.com.au"],
        "gov":["defence.gov.au","pm.gov.au","dfat.gov.au","treasury.gov.au"]
    },
    "new zealand": {
        "lang":"en",
        "news":["rnz.co.nz","nzherald.co.nz","stuff.co.nz","newshub.co.nz"],
        "gov":["beehive.govt.nz","mfat.govt.nz","nzdf.mil.nz","treasury.govt.nz"]
    },
    "japan": {
        "lang":"ja",
        "news":["nhk.or.jp","asahi.com","yomiuri.co.jp","mainichi.jp","nikkei.com"],
        "gov":["mod.go.jp","mofa.go.jp","kantei.go.jp","cao.go.jp"]
    },
    "south korea": {
        "lang":"ko",
        "news":["yna.co.kr","koreaherald.com","koreatimes.co.kr","chosun.com","joongang.co.kr","hani.co.kr"],
        "gov":["mnd.go.kr","mofa.go.kr","pmo.go.kr","moef.go.kr"]
    },
    "taiwan": {
        "lang":"zh",
        "news":["cna.com.tw","focustaiwan.tw","taipeitimes.com","taiwannews.com.tw","ltn.com.tw","udn.com","chinatimes.com","setn.com","storm.mg"],
        "gov":["mnd.gov.tw","mofa.gov.tw","president.gov.tw","ey.gov.tw","mof.gov.tw"]
    },
    "singapore": {
        "lang":"en",
        "news":["straitstimes.com","channelnewsasia.com","todayonline.com"],
        "gov":["mindef.gov.sg","mfa.gov.sg","pmo.gov.sg"]
    },

    # EU Big 5 + DACH + Nordics + Baltics + CEE
    "germany": {
        "lang":"de",
        "news":["tagesschau.de","zeit.de","faz.net","spiegel.de","sueddeutsche.de","handelsblatt.com","welt.de"],
        "gov":["bundesregierung.de","bmvg.de","auswaertiges-amt.de","bmz.de","bmf.de","kfw.de"]
    },
    "france": {
        "lang":"fr",
        "news":["lemonde.fr","lefigaro.fr","liberation.fr","lesechos.fr","france24.com"],
        "gov":["gouvernement.fr","elysee.fr","diplomatie.gouv.fr","defense.gouv.fr","economie.gouv.fr"]
    },
    "italy": {
        "lang":"it",
        "news":["repubblica.it","corriere.it","ansa.it","ilsole24ore.com","rainews.it"],
        "gov":["governo.it","esteri.it","difesa.it","mef.gov.it","aics.gov.it"]
    },
    "spain": {
        "lang":"es",
        "news":["elpais.com","elmundo.es","abc.es","lavanguardia.com","rtve.es"],
        "gov":["lamoncloa.gob.es","exteriores.gob.es","defensa.gob.es","hacienda.gob.es"]
    },
    "netherlands": {
        "lang":"nl",
        "news":["nos.nl","nrc.nl","volkskrant.nl","trouw.nl","telegraaf.nl"],
        "gov":["rijksoverheid.nl","defensie.nl","minbuza.nl","minfin.nl"]
    },
    "belgium": {
        "lang":"fr",
        "news":["rtbf.be","lavenir.net","lesoir.be","standaard.be","vrt.be"],
        "gov":["belgium.be","fgov.be","defense.belgium.be","diplomatie.belgium.be"]
    },
    "luxembourg": {
        "lang":"fr",
        "news":["wort.lu","rtl.lu"],
        "gov":["gouvernement.lu","maee.gouvernement.lu","army.lu"]
    },
    "ireland": {
        "lang":"en",
        "news":["rte.ie","irishtimes.com","independent.ie","thejournal.ie"],
        "gov":["gov.ie","dfa.ie","defence.ie","finance.gov.ie"]
    },
    "portugal": {
        "lang":"pt",
        "news":["publico.pt","expresso.pt","observador.pt","rtp.pt"],
        "gov":["portugal.gov.pt","defesa.gov.pt","mne.gov.pt","portaldasfinancas.gov.pt"]
    },
    "greece": {
        "lang":"el",
        "news":["ekathimerini.com","tovima.gr","naftemporiki.gr","protothema.gr","ertnews.gr"],
        "gov":["government.gov.gr","mfa.gr","mod.mil.gr","minfin.gov.gr"]
    },
    "austria": {
        "lang":"de",
        "news":["orf.at","derstandard.at","diepresse.com","kurier.at","kleinezeitung.at","profil.at"],
        "gov":["bka.gv.at","bmeia.gv.at","bundesheer.at","bmlv.gv.at","bmf.gv.at","parlament.gv.at"]
    },
    "switzerland": {
        "lang":"de",
        "news":["srf.ch","nzz.ch","tagesanzeiger.ch","letemps.ch"],
        "gov":["admin.ch","vbs.admin.ch","eda.admin.ch"]
    },
    "poland": {
        "lang":"pl",
        "news":["tvn24.pl","wyborcza.pl","rp.pl","onet.pl","polsatnews.pl"],
        "gov":["gov.pl","mon.gov.pl","msz.gov.pl","kprm.gov.pl","mf.gov.pl"]
    },
    "czech republic": {
        "lang":"cs",
        "news":["idnes.cz","seznamzpravy.cz","novinky.cz","hn.cz","ct24.ceskatelevize.cz"],
        "gov":["vlada.cz","mzv.cz","army.cz","mofcr.cz"]
    },
    "slovakia": {
        "lang":"sk",
        "news":["sme.sk","pravda.sk","aktuality.sk","tasr.sk","teraz.sk"],
        "gov":["gov.sk","mfa.sk","mod.gov.sk","finance.gov.sk"]
    },
    "hungary": {
        "lang":"hu",
        "news":["index.hu","telex.hu","444.hu","hvg.hu"],
        "gov":["kormany.hu","honvedelem.hu","mfa.gov.hu","pm.gov.hu"]
    },
    "romania": {
        "lang":"ro",
        "news":["agerpres.ro","hotnews.ro","digi24.ro","adevarul.ro","g4media.ro"],
        "gov":["gov.ro","mae.ro","mapn.ro","mfinante.gov.ro"]
    },
    "bulgaria": {
        "lang":"bg",
        "news":["bntnews.bg","dnevnik.bg","novinite.com","24chasa.bg"],
        "gov":["government.bg","mfa.bg","mod.bg","minfin.bg"]
    },
    "slovenia": {
        "lang":"sl",
        "news":["rtvslo.si","delo.si","vecer.com"],
        "gov":["gov.si","mzv.gov.si","mo.gov.si","mf.gov.si"]
    },
    "croatia": {
        "lang":"hr",
        "news":["hrt.hr","vecernji.hr","jutarnji.hr","index.hr"],
        "gov":["gov.hr","mvep.gov.hr","morh.hr","mfin.gov.hr"]
    },
    "estonia": {
        "lang":"et",
        "news":["err.ee","postimees.ee","delfi.ee"],
        "gov":["valitsus.ee","mfa.ee","kaitseministeerium.ee","mil.ee"]
    },
    "latvia": {
        "lang":"lv",
        "news":["lsm.lv","delfi.lv","tvnet.lv"],
        "gov":["mk.gov.lv","mfa.gov.lv","mod.gov.lv","fm.gov.lv"]
    },
    "lithuania": {
        "lang":"lt",
        "news":["lrt.lt","15min.lt","delfi.lt"],
        "gov":["lrv.lt","urm.lt","kam.lt","finmin.lrv.lt"]
    },
    "finland": {
        "lang":"fi",
        "news":["yle.fi","hs.fi","iltalehti.fi","iltasanomat.fi"],
        "gov":["valtioneuvosto.fi","defmin.fi","puolustusvoimat.fi","um.fi"]
    },
    "sweden": {
        "lang":"sv",
        "news":["svt.se","dn.se","svd.se","aftonbladet.se","expressen.se"],
        "gov":["regeringen.se","gov.se","swedenabroad.se","forsvarsmakten.se"]
    },
    "denmark": {
        "lang":"da",
        "news":["dr.dk","politiken.dk","jyllands-posten.dk","berlingske.dk","tv2.dk"],
        "gov":["um.dk","fmn.dk","ft.dk","fm.dk"]
    },
    "norway": {
        "lang":"no",
        "news":["nrk.no","vg.no","aftenposten.no","dagbladet.no"],
        "gov":["regjeringen.no","forsvaret.no","ud.dep.no","statsbudsjettet.no"]
    },
    "iceland": {
        "lang":"is",
        "news":["ruv.is","mbl.is","visir.is"],
        "gov":["gov.is","mfa.is","althingi.is"]
    },
    "cyprus": {
        "lang":"el",
        "news":["cyprus-mail.com","politis.com.cy","sigmalive.com"],
        "gov":["cyprus.gov.cy","mfa.gov.cy","mod.gov.cy","mof.gov.cy"]
    },
    "malta": {
        "lang":"mt",
        "news":["timesofmalta.com","tvmnews.mt","independent.com.mt"],
        "gov":["gov.mt","foreign.gov.mt","defence.gov.mt","mfin.gov.mt"]
    },

    # Turkey & MENA
    "turkey": {
        "lang":"tr",
        "news":["aa.com.tr","hurriyet.com.tr","sabah.com.tr","sozcu.com.tr","ntv.com.tr","haberturk.com","trthaber.com","yenisafak.com"],
        "gov":["msb.gov.tr","mfa.gov.tr","tccb.gov.tr","resmigazete.gov.tr","savunmasanayi.gov.tr"]
    },
    "united arab emirates": {
        "lang":"ar",
        "news":["thenationalnews.com","wam.ae"],
        "gov":["mofaic.gov.ae","mod.gov.ae","uae-embassy.org"]
    },
    "saudi arabia": {
        "lang":"ar",
        "news":["arabnews.com","spa.gov.sa","alriyadh.com"],
        "gov":["mofa.gov.sa","mod.gov.sa"]
    },
    "qatar": {
        "lang":"ar",
        "news":["gulf-times.com","aljazeera.com","qna.org.qa"],
        "gov":["mofa.gov.qa","qna.org.qa"]
    },
    "kuwait": {
        "lang":"ar",
        "news":["kuwaittimes.com","kuna.net.kw"],
        "gov":["mofa.gov.kw","kuna.net.kw"]
    },

    # Americas / others
    "brazil": {
        "lang":"pt",
        "news":["g1.globo.com","folha.uol.com.br","estadao.com.br"],
        "gov":["gov.br","itamaraty.gov.br","defesa.gov.br","fazenda.gov.br"]
    },
    "mexico": {
        "lang":"es",
        "news":["eluniversal.com.mx","reforma.com","milenio.com"],
        "gov":["gob.mx","sre.gob.mx","sedena.gob.mx","hacienda.gob.mx"]
    },
    "south africa": {
        "lang":"en",
        "news":["news24.com","iol.co.za","businesstech.co.za"],
        "gov":["gov.za","dirco.gov.za","dod.mil.za","treasury.gov.za"]
    },
    "israel": {
        "lang":"he",
        "news":["timesofisrael.com","haaretz.com","ynetnews.com"],
        "gov":["mfa.gov.il","mod.gov.il","idf.il"]
    },
    "india": {
        "lang":"en",
        "news":["thehindu.com","timesofindia.indiatimes.com","indianexpress.com","hindustantimes.com"],
        "gov":["mea.gov.in","mod.gov.in","pib.gov.in","finmin.nic.in"]
    },
}

COUNTRY_LANG = {
    "united states":"en","united kingdom":"en","canada":"en","australia":"en","new zealand":"en",
    "japan":"ja","south korea":"ko","taiwan":"zh","singapore":"en",
    "germany":"de","france":"fr","italy":"it","spain":"es","portugal":"pt","netherlands":"nl",
    "belgium":"fr","luxembourg":"fr","ireland":"en","austria":"de","switzerland":"de",
    "poland":"pl","czech republic":"cs","slovakia":"sk","hungary":"hu","romania":"ro","bulgaria":"bg",
    "slovenia":"sl","croatia":"hr","estonia":"et","latvia":"lv","lithuania":"lt",
    "finland":"fi","sweden":"sv","denmark":"da","norway":"no","iceland":"is","cyprus":"el","malta":"mt",
    "turkey":"tr","united arab emirates":"ar","saudi arabia":"ar","qatar":"ar","kuwait":"ar",
    "brazil":"pt","mexico":"es","south africa":"en","israel":"he","india":"en"
}

COUNTRY_ALIASES = {
    "usa":"united states","us":"united states","united states of america":"united states",
    "uk":"united kingdom","great britain":"united kingdom","britain":"united kingdom","gb":"united kingdom",
    "republic of korea":"south korea","korea, republic of":"south korea","rok":"south korea",
    "czechia":"czech republic","uae":"united arab emirates","u.a.e.":"united arab emirates",
    "korea":"south korea", "roc":"taiwan","taipei":"taiwan"
}

def _normalize_country_key(country: str) -> str:
    key = (country or "").strip().lower()
    key = COUNTRY_ALIASES.get(key, key)
    return key

def _country_profile(country: str) -> Dict:
    key = (country or "").strip().lower()
    key = COUNTRY_ALIASES.get(key, key) if 'COUNTRY_ALIASES' in globals() else key
    prof = COUNTRY_PROFILES.get(key, {"lang":"en","news":GENERIC_NEWS, "gov":GENERIC_GOV})
    news = list(dict.fromkeys(prof.get("news", []) + OSINT))
    gov  = list(dict.fromkeys(prof.get("gov",  []) + EU_INTL_NEWS + UA_GOV + SECURITY))
    return {"lang": COUNTRY_LANG.get(key, prof.get("lang","en")), "news": news, "gov": gov}

def donors_list(df: pd.DataFrame) -> Tuple[str, List[str]]:
    cols = [c for c in df.columns if _norm(c) in ("donor","provider","country","country_donor")]
    donor_col = cols[0] if cols else df.columns[0]
    donors = sorted(df[donor_col].dropna().astype(str).unique().tolist())
    return donor_col, donors

def norm_measure(x) -> str:
    s = str(x).lower()
    if "deliver" in s:   return "delivery"
    if "disburse" in s:  return "disbursement"
    if "alloc" in s:     return "allocation"
    if "commit" in s:    return "commitment"
    if "announce" in s or "pledge" in s: return "announcement"
    return s

MIL_RX   = re.compile(r"(milit|defen[cs]e|weapon|ammo|munition|artiller|howitzer|mortar|missile|rocket|uav|drone|loitering|tank|armou|apc|ifv|mbt|rifle|small\s*arms|sam|patriot|nasams|himars|gmlrs|radar|night\s*vision|helmet|vest|body\s*armor|generator|diesel|fuel)", re.I)
HUM_RX   = re.compile(r"(humanitarian|medical|health|hospital|ambulance|shelter|refugee|ngo|medicine|food|winteri[sz]ation)", re.I)
LOAN_RX  = re.compile(r"(loan|credit|guarantee|bond|facility|macro[-\s]?financial|budget\s*support|reconstruction|coebank|kfw|eib|world\s*bank)", re.I)

def bucket_row(tg, ts, ex):
    tg = (tg or "").lower(); ts = (ts or "").lower(); ex = (ex or "").lower()
    if LOAN_RX.search(ex) or "budget" in tg or "reconstruction" in tg:
        return "loans_non_military"
    if "humanitarian" in tg or "humanitarian" in ts or HUM_RX.search(ex):
        return "direct_humanitarian_aid"
    if "milit" in tg or "defen" in tg or "security" in tg: return "military_inventory_transfer"
    if "milit" in ts or "weapon" in ts or "munition" in ts or "equipment" in ts: return "military_inventory_transfer"
    if MIL_RX.search(ex): return "military_inventory_transfer"
    return "other"

def build_kiel_slice(df_main: pd.DataFrame, country: str) -> Tuple[pd.DataFrame, Dict[str,str]]:
    hdr = {_norm(c): c for c in df_main.columns}
    donor = _pick(hdr, "donor","provider","country")
    date  = _pick(hdr, "announcement_date")
    curr  = _pick(hdr, "reporting_currency")
    meas  = _pick(hdr, "measure")
    type_g= _pick(hdr, "aid_type_general","type_general")
    type_s= _pick(hdr, "aid_type_specific","type_specific")
    expl  = _pick(hdr, "explanation")
    item  = _pick(hdr, "item", contains_ok=False)
    act_val_eur = _pick(hdr, "tot_activity_value_eur")
    sub_val_eur = _pick(hdr, "tot_sub_activity_value_eur", contains_ok=False)
    value_deliv_eur = _pick(hdr, "tot_value_deliv_eur")
    act_id = _pick(hdr, "activity_id","aid_id","id", contains_ok=True)

    for c in [donor,date,curr,meas,type_g,type_s,expl,item,act_val_eur,sub_val_eur,value_deliv_eur,act_id]:
        if not c or c not in df_main.columns:
            df_main[c or "missing"] = np.nan

    d = df_main[df_main[donor].astype(str).str.contains(country, case=False, na=False)].copy()
    d[date] = pd.to_datetime(d[date], errors="coerce")
    d["month"] = d[date].dt.to_period("M").astype(str)
    d["measure_norm"] = d[meas].map(norm_measure)
    d["delivered_disbursed"] = d["measure_norm"].isin(["delivery","disbursement"])

    val_eur_col = act_val_eur or sub_val_eur
    d["value_eur"] = pd.to_numeric(d[val_eur_col], errors="coerce") if val_eur_col else np.nan
    d["value_delivered_eur"] = pd.to_numeric(d[value_deliv_eur], errors="coerce") if value_deliv_eur else np.nan

    d["bucket"] = [
        bucket_row(
            d.loc[i, type_g] if type_g in d.columns else "",
            d.loc[i, type_s] if type_s in d.columns else "",
            d.loc[i, expl]   if expl   in d.columns else ""
        )
        for i in d.index
    ]
    d["value_eur"] = d["value_eur"].round(2)
    d["value_delivered_eur"] = d["value_delivered_eur"].round(2)

    meta = {"donor": donor, "date": date, "curr": curr, "measure": meas,
            "type_g": type_g, "type_s": type_s, "expl": expl, "item": item, "act_id": act_id}
    return d, meta

# ---------- search URL builder ----------

def _search_urls_for_row(row: pd.Series, meta: Dict[str,str], country: str) -> List[str]:
    prof = _country_profile(country)
    lang = prof["lang"]
    ts = str(row.get(meta["type_s"], ""))[:100]
    tg = str(row.get(meta["type_g"], ""))[:100]
    m  = str(row.get("month",""))
    amt = row.get("value_eur")
    base_en = f'{country} Ukraine {ts or tg} {m} {amt if pd.notna(amt) else ""}'.strip()
    base_en = re.sub(r"\s+"," ", base_en)
    base_local = _tr(base_en, lang)

    domains = prof["gov"] + prof["news"] + EU_INTL_NEWS + IFIS + UA_GOV + SECURITY
    urls = []
    q_en = "+".join(base_en.split())
    for d in domains:
        urls.append(f"https://www.google.com/search?q={q_en}+site%3A{d}")
    if base_local != base_en:
        q_loc = "+".join(base_local.split())
        for d in domains:
            urls.append(f"https://www.google.com/search?q={q_loc}+site%3A{d}")
    urls.append(f"https://www.google.com/search?q={q_en}+Ukraine")
    if base_local != base_en:
        urls.append(f"https://www.google.com/search?q={'+'.join(base_local.split())}+Ukraine")
    return urls[:20]

USER_AGENT = "Mozilla/5.0 (compatible; AidScraper/1.0)"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en;q=0.8"}

DELIVERY_WORDS = r"(delivered|handed\s+over|arriv(?:ed|als?)|transferred|shipment|shipped|supplied|provided)"
COMMIT_WORDS   = r"(announce(?:d|ment)|pledge(?:d)?|commit(?:ted|ment)|authorize(?:d)?)"
MONTH_FMT = "%Y-%m"

def _is_google_search(u: str) -> bool:
    try:
        host = urlparse(u).hostname or ""
    except Exception:
        return False
    return "google." in host and "/search" in u

def _good_url(u: str) -> Optional[str]:
    if not isinstance(u, str): return None
    u = u.strip()
    if not u.startswith("http"): return None
    return u

def _cache_path(u: str) -> Path:
    h = hashlib.sha1(u.encode("utf-8")).hexdigest()
    p = Path(".aidscrape_cache"); p.mkdir(exist_ok=True)
    return p / f"{h}.json"

def _save_cache(key: str, payload: Dict):
    try:
        _cache_path(key).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _read_cache(key: str) -> Optional[Dict]:
    p = _cache_path(key)
    if p.exists():
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return None
    return None

def _google_first_result(search_url: str) -> Optional[str]:
    """Follow a Google search URL and grab the first organic result."""
    if not HAVE_WEB: return None
    cached = _read_cache("g:" + search_url)
    if cached: return cached.get("url")
    try:
        r = requests.get(search_url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a"):
            href = a.get("href","")
            # organic results look like /url?q=https://target&...
            if href.startswith("/url?"):
                q = parse_qs(urlparse(href).query).get("q", [""])[0]
                if q.startswith("http") and "google." not in (urlparse(q).hostname or ""):
                    _save_cache("g:" + search_url, {"url": q})
                    return q
        return None
    except Exception:
        return None

def fetch_text(url: str, timeout: int = 25) -> Tuple[str, str]:
    if not HAVE_WEB: return ("","")
    cached = _read_cache(url)
    if cached: return (cached.get("kind",""), cached.get("text",""))
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        ctype = r.headers.get("Content-Type","").lower()
        if "application/pdf" in ctype or url.lower().endswith(".pdf"):
            if pdf_extract_text is None: return ("","")
            text = pdf_extract_text(io.BytesIO(r.content)) or ""
            _save_cache(url, {"kind":"pdf","text":text})
            return ("pdf", text)
        soup = BeautifulSoup(r.text, "lxml")
        for s in soup(["script","style","noscript"]): s.decompose()
        text = " ".join(soup.get_text(separator=" ").split())
        pub = ""
        mt = soup.find("meta", attrs={"property":"article:published_time"}) or soup.find("time")
        if mt:
            pub = mt.get("content") or mt.get("datetime") or mt.text
            if pub: text = f"Published {pub}. " + text
        _save_cache(url, {"kind":"html","text":text})
        return ("html", text)
    except Exception:
        return ("", "")

def _month_from_text(text: str, country: str) -> Optional[str]:
    if search_dates is None: return None
    lang = COUNTRY_LANG.get(country.strip().lower(), "en")
    try:
        hits = search_dates(text, languages=[lang,"en"], settings={"RETURN_AS_TIMEZONE_AWARE": False})
    except Exception:
        hits = None
    if not hits: return None
    for _, dt in hits:
        if 2022 <= dt.year <= 2026:
            return dt.strftime(MONTH_FMT)
    return None

def infer_status(text: str) -> str:
    t = text.lower()
    if re.search(DELIVERY_WORDS, t):
        return "Delivered/Disbursed"
    if re.search(COMMIT_WORDS, t):
        return "Commitment/Other"
    return "Commitment/Other"

def source_type(text: str) -> str:
    t = text.lower()
    if re.search(r"(drawdown|from\s+stocks?|from\s+stockpiles|from\s+inventory|from\s+reserves|pda|presidential\s+drawdown)", t): 
        return "stockpile"
    if re.search(r"(procure|procurement|contract|order|purchase|manufactur|framework\s+contract|tender)", t):
        return "new_production"
    if re.search(r"(ringtausch|backfill|swap|indirect\s+transfer|compensat(?:e|ion)|in\s+return)", t):
        return "indirect"
    return "unknown"

def useful_life_years(items_text: str) -> int:
    t = (items_text or "").lower()
    if re.search(r"(ammo|ammunition|shell|round|rocket|missile|grenade|mine)", t): return 0
    if re.search(r"(howitzer|artillery|mortar|gun)", t): return 30
    if re.search(r"(tank|mbt|ifv|apc|armou|vehicle|truck|stryk|bradley|leopard|abrams|m113|cv90)", t): return 25
    if re.search(r"(patriot|nasams|iris[-\s]?t|sam|air\s*defen|radar)", t): return 25
    if re.search(r"(uav|drone|loitering|phoenix\s*ghost|switchblade)", t): return 6
    if re.search(r"(night\s*vision|nvg|helmet|vest|uniform|protective|generator)", t): return 5
    if re.search(r"(demin|clearance)", t): return 10
    return 15

def extract_items(text: str) -> List[str]:
    KEYMAP = [
        (r"\bATACMS\b", "ATACMS long-range missiles"),
        (r"\bHIMARS\b", "HIMARS rockets/launchers"),
        (r"\bGMLRS\b", "GMLRS rockets"),
        (r"\bNASAMS?\b", "NASAMS air-defense"),
        (r"\bPatriot\b", "Patriot air-defense"),
        (r"\bAIM[-\s]?9\w*\b", "AIM-9 missiles"),
        (r"\bAMRAAM\b|\bAIM[-\s]?120\b", "AMRAAM missiles"),
        (r"\bF[-\s]?16\b", "F-16 support"),
        (r"\bLeopard\b", "Leopard tanks"),
        (r"\bAbrams\b|\bBradley\b|\bStryker\b|\bM113\b|\bCV90\b", "Armored vehicles"),
        (r"\b(155|152|122|120|105)\s?mm\b", "Artillery/mortar ammo"),
        (r"\bSwitchblade\b|\bPhoenix\s+Ghost\b", "Loitering munitions"),
        (r"\bStorm\s+Shadow\b|\bSCALP\b", "Cruise missiles"),
        (r"\bStinger\b|\bJavelin\b|\bNLAW\b", "AT/AA missiles"),
        (r"night\s*vision|helmet|vest|generator|ambulance", "Non-lethal equipment"),
        (r"demin|clearance", "Demining equipment"),
        (r"\bPT[-\s]?91\b", "PT-91 tanks"),
        (r"\bT[-\s]?72\b", "T-72 tanks"),
        (r"\bMi[-\s]?24\b", "Mi-24 attack helicopters"),
        (r"\bPiorun\b", "Piorun MANPADS"),
        (r"\bZU[-\s]?23[-\s]?2\b", "ZU-23-2 autocannons"),
        (r"\bFlyEye\b", "FlyEye UAVs"),
        (r"\bS[-\s]?60\b", "S-60 AA guns"),
        (r"\bLMP[-\s]?2017\b", "LMP-2017 mortars"),
    ]
    out, low = [], text.lower()
    for rx, label in KEYMAP:
        if re.search(rx, low, flags=re.IGNORECASE):
            out.append(label)
    return list(dict.fromkeys(out))[:20]

ITEM_NOUNS = r"(?:units?|systems?|vehicles?|tanks?|ifvs?|apcs?|mbts?|howitzers?|guns?|launchers?|batteries?|missiles?|rockets?|rounds?|shells?|cartridges?|grenades?|mines?|drones?|uavs?|aircraft|helicopters?|rifles?|trucks?|radars?)"

def extract_item_counts(text: str) -> List[str]:
    rx = re.compile(
        rf'(?P<qty>\d{{1,3}}(?:[.,]\d{{3}})*|\d+)'
        rf'\s*(?:x\s*)?(?P<noun>{ITEM_NOUNS})?\s*'
        rf'(?P<item>(?:[A-Z][\w\-/]+(?:\s+[A-Z0-9][\w\-/]+){{0,3}}|\d{{2,4}}\s?mm(?:\s*(?:rounds?|shells?|ammo))?))',
        flags=re.IGNORECASE
    )
    out = []
    for m in rx.finditer(text):
        qty = int(_to_float(m.group('qty')))
        noun = (m.group('noun') or '').strip()
        item = (m.group('item') or '').strip()
        label = f"{qty} {item}" if not noun else f"{qty} {item} {noun}"
        if 1 <= qty and len(label) <= 80:
            out.append(label)
    # de-dup
    uniq = []
    for x in out:
        if x not in uniq:
            uniq.append(x)
    return uniq[:20]

UNIT_COST_EUR = [
    (r'\b155\s?mm\b.*(round|shell|ammo)', 3_500),
    (r'\b105\s?mm\b.*(round|shell|ammo)', 2_500),
    (r'\b120\s?mm\b.*mortar', 1_200),
    (r'\bGMLRS\b', 160_000),
    (r'\bATACMS\b', 1_200_000),
    (r'\bHIMARS\b.*(launcher|system)?', 5_000_000),
    (r'\bPatriot\b.*(battery|system)?', 400_000_000),
    (r'\bNASAMS?\b', 80_000_000),
    (r'\bAMRAAM\b|\bAIM[-\s]?120\b', 1_200_000),
    (r'\bAIM[-\s]?9\w*\b', 400_000),
    (r'\bStinger\b', 120_000),
    (r'\bJavelin\b', 170_000),
    (r'\bNLAW\b', 40_000),
    (r'\bBradley\b', 3_500_000),
    (r'\bStryker\b', 4_500_000),
    (r'\bAbrams\b', 9_000_000),
    (r'\bLeopard\s?2\w*\b', 8_000_000),
    (r'\bLeopard\b', 4_000_000),
    (r'\bM113\b', 300_000),
    (r'\bCV90\b', 8_000_000),
    (r'\bMi-24\b', 10_000_000),
    (r'\bFlyEye\b', 300_000),
    (r'\bPiorun\b', 100_000),
    (r'\bradar\b', 5_000_000),
    (r'\bhowitzer\b', 1_000_000),
    (r'\bmortar\b', 120_000),
    (r'\bAPC\b|\bIFV\b|\barmou?red?\s+vehicle\b', 1_500_000),
    (r'\btruck\b', 150_000),
    (r'\bdrones?\b|\bUAVs?\b', 50_000),
    (r'\brifles?\b', 1_200),
    (r'\bhelmets?\b|\bvests?\b', 400),
]

def _unit_cost_eur(label: str) -> Optional[float]:
    for rx, price in UNIT_COST_EUR:
        if re.search(rx, label, re.IGNORECASE):
            return float(price)
    return None

def _parse_qty_item(label: str) -> Optional[Tuple[int, str]]:
    m = re.match(r'^\s*(\d[\d,\.]*)\s+(.+?)\s*$', label)
    if not m:
        return None
    qty = int(_to_float(m.group(1)))
    what = m.group(2)
    return qty, what

def estimate_value_from_count_labels(labels: List[str]) -> Tuple[float, List[str]]:
    total = 0.0
    breakdown = []
    for lab in labels:
        p = _parse_qty_item(lab)
        if not p:
            continue
        qty, what = p
        unit = _unit_cost_eur(what)
        if unit:
            val = qty * unit
            total += val
            breakdown.append(f"{qty}×{what} @≈€{unit:,.0f} ≈ €{val:,.0f}")
    return total, breakdown

def estimate_value_from_text(text: str) -> Tuple[float, List[str]]:
    return estimate_value_from_count_labels(extract_item_counts(text))

def parse_source(url: str, country: str) -> Dict[str, Optional[str]]:
    kind, txt = fetch_text(url)
    if not txt:
        return {"status": None, "evidence_month": None, "items": None,
                "source_type": None, "raw_text": "", "value_eur": None, "money_evidence": None}

    status = infer_status(txt)
    win = None
    m = re.search(rf".{{0,200}}{DELIVERY_WORDS}.{{0,200}}", txt, flags=re.IGNORECASE)
    if m: win = _month_from_text(m.group(0), country)
    if not win: win = _month_from_text(txt[:1200] + txt[-1200:], country)

    items_quant = extract_item_counts(txt)
    items_simple = extract_items(txt)
    items = items_quant if items_quant else items_simple

    s_type = source_type(txt)
    val_eur, cur, money_frag = extract_money_eur(txt)

    return {
        "status": status,
        "evidence_month": win,
        "items": "; ".join(items),
        "source_type": s_type,
        "raw_text": txt[:20000],
        "value_eur": val_eur,
        "money_evidence": money_frag
    }

# ---------- sheets builders ----------
def to_military_raw(df: pd.DataFrame, meta: Dict[str,str], country: str) -> pd.DataFrame:
    d = df[df["bucket"]=="military_inventory_transfer"].copy()
    if d.empty:
        cand = df[df["bucket"].isin(["other", None])].copy()
        mask = (
            cand[meta["type_g"]].astype(str).str.contains(r"milit|defen|security", case=False, na=False) |
            cand[meta["type_s"]].astype(str).str.contains(r"milit|weapon|ammo|munition|equipment", case=False, na=False) |
            cand[meta["expl"]].astype(str).str.contains(MIL_RX, na=False)
        )
        d = cand[mask].copy()

    rows = []
    for _, r in d.iterrows():
        base_val = pd.to_numeric(r.get("value_delivered_eur"), errors="coerce")
        if not pd.notna(base_val) or float(base_val) == 0.0:
            base_val = pd.to_numeric(r.get("value_eur"), errors="coerce")
        base_val = float(base_val) if pd.notna(base_val) else np.nan

        desc = (str(r.get(meta["item"], "")) or str(r.get(meta["expl"], ""))).strip()
        urls = _search_urls_for_row(r, meta, country)
        src = " | ".join(urls[:3])

        rows.append({
            "Month": r.get("month",""),
            "Base Value (EUR)": base_val,
            "Notable Weapons and Munitions Delivered (via PDA shipments)": desc,
            "Status": "",
            "Evidence Month": "",
            "Sources": src,
            "Source Type": "",
            "Production Year": "",
            "Useful Life (yrs)": "",
            "Training Value (EUR)": "",
            "Final Depreciated Value (EUR)": "",
        })
    return pd.DataFrame(rows)

def _resolve_url_candidates(src: str) -> List[str]:
    out = []
    if not isinstance(src, str): return out
    for token in re.split(r"\s*\|\s*", src):
        token = token.strip()
        if not token: continue
        if _is_google_search(token):
            u = _google_first_result(token)
            if u: out.append(u)
        elif token.startswith("http"):
            out.append(token)
    # unique
    uniq = []
    for u in out:
        if u not in uniq: uniq.append(u)
    return uniq[:2]

def auto_enrich_military(df_raw: pd.DataFrame, country: str, max_rows: Optional[int]=None) -> pd.DataFrame:
    if not HAVE_WEB or df_raw.empty:
        return df_raw

    rows = df_raw.to_dict("records")
    limit = max_rows or int(os.environ.get("AID_SCRAPE_LIMIT", "16") or "16")
    threads = max(1, int(os.environ.get("AID_THREADS", "6") or "6"))

    order = sorted(range(len(rows)),
                   key=lambda i: float(pd.to_numeric(rows[i].get("Base Value (EUR)"), errors="coerce") or 0.0),
                   reverse=True)[:limit]

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def work(i: int):
        row = rows[i]
        cands = _resolve_url_candidates(row.get("Sources",""))
        for u in cands:
            r = parse_source(u, country)
            if any([r.get("status"), r.get("evidence_month"), r.get("items"), r.get("value_eur")]):
                return (i, r)
        return (i, None)

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(work, i) for i in order]
        for fut in as_completed(futs):
            i, res = fut.result()
            if not res: 
                continue
            row = rows[i]

            if res.get("items"):
                old = (row.get("Notable Weapons and Munitions Delivered (via PDA shipments)") or "").strip()
                add = res["items"].strip()
                if add and add not in old:
                    row["Notable Weapons and Munitions Delivered (via PDA shipments)"] = (old + ("; " if old else "") + add)[:6000]

            row["Status"] = row.get("Status") or res.get("status") or ""
            row["Evidence Month"] = row.get("Evidence Month") or res.get("evidence_month") or ""
            row["Source Type"] = row.get("Source Type") or res.get("source_type") or ""

            base_val = pd.to_numeric(row.get("Base Value (EUR)"), errors="coerce")
            if not (pd.notna(base_val) and float(base_val) > 0):
                if res.get("value_eur"):
                    row["Base Value (EUR)"] = float(res["value_eur"])
                else:
                    est_val, _br = estimate_value_from_text(res.get("raw_text", "") or "")
                    if est_val and est_val > 0:
                        row["Base Value (EUR)"] = float(est_val)

            items_text = str(row.get("Notable Weapons and Munitions Delivered (via PDA shipments)") or "")
            life = useful_life_years(items_text)
            row["Useful Life (yrs)"] = life if life else (0 if life == 0 else "")

            base_val = float(pd.to_numeric(row.get("Base Value (EUR)"), errors="coerce") or 0.0)
            try:
                send_year = int((row.get("Evidence Month") or row.get("Month") or "")[:4])
            except Exception:
                send_year = None

            if base_val > 0:
                if send_year is not None and (row.get("Source Type") or "unknown") == "stockpile":
                    prod_year = send_year - min(max(1, (life or 1)//2), 12)
                    years_used = max(0, send_year - prod_year)
                    annual = base_val / (life or 1)
                    row["Final Depreciated Value (EUR)"] = max(0.0, base_val - annual * years_used)
                else:
                    row["Final Depreciated Value (EUR)"] = base_val

            rows[i] = row

    return pd.DataFrame(rows)

def _bootstrap_queries(country: str) -> List[str]:
    prof = _country_profile(country)
    lang = prof["lang"]
    base_en = [
        "Ukraine military aid package",
        "Ukraine weapons donation",
        "Ukraine defense support",
        "contract procurement Ukraine",
        "delivered to Ukraine weapons"
    ]
    base_local = [_tr(q, lang) for q in base_en]
    queries = list(dict.fromkeys(base_en + base_local))
    out = []
    for q in queries:
        q_plus = "+".join(q.split())
        for d in (prof["gov"] + prof["news"]):
            out.append(f"https://www.google.com/search?q={q_plus}+site%3A{d}")
    return out[:40]

def _build_rows_from_web(country: str, limit: int = 18) -> pd.DataFrame:
    if not HAVE_WEB:
        return pd.DataFrame(columns=[
            "Month","Base Value (EUR)","Notable Weapons and Munitions Delivered (via PDA shipments)",
            "Status","Evidence Month","Sources","Source Type","Production Year",
            "Useful Life (yrs)","Training Value (EUR)","Final Depreciated Value (EUR)"
        ])
    rows = []
    seen = set()
    for s in _bootstrap_queries(country):
        u = _google_first_result(s)
        if not u or u in seen:
            continue
        seen.add(u)
        res = parse_source(u, country)
        desc = res.get("items") or ""
        life = useful_life_years(desc)

        base = float(res.get("value_eur") or 0.0)
        if (not base) and res.get("raw_text"):
            est_val, _ = estimate_value_from_text(res["raw_text"])
            if est_val:
                base = float(est_val)

        final = base
        if base and (res.get("source_type") == "stockpile"):
            annual = base / (life or 1)
            final = max(0.0, base - annual * max(1, (life or 1)//2))
            
        if not any([res.get("evidence_month"), res.get("items"), res.get("value_eur")]):
            continue
        desc = res.get("items") or ""
        life = useful_life_years(desc)
        base = float(res.get("value_eur") or 0.0)
        final = base
        if base and (res.get("source_type") == "stockpile"):
            annual = base / (life or 1)
            final = max(0.0, base - annual * max(1, (life or 1)//2))
        rows.append({
            "Month": res.get("evidence_month") or "",
            "Base Value (EUR)": base if base else np.nan,
            "Notable Weapons and Munitions Delivered (via PDA shipments)": desc,
            "Status": res.get("status") or "",
            "Evidence Month": res.get("evidence_month") or "",
            "Sources": u,
            "Source Type": res.get("source_type") or "",
            "Production Year": "",
            "Useful Life (yrs)": life if life else "",
            "Training Value (EUR)": "",
            "Final Depreciated Value (EUR)": final if base else np.nan,
        })
        if len(rows) >= limit:
            break
    return pd.DataFrame(rows)


# --- cleaner for descriptions in MIT (keep calibers; drop money/quantities) ---
def _clean_desc_for_agg(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return ""
    text = s
    calibers = []
    def _cap(m):
        calibers.append(m.group(0))
        return f"__CAL{len(calibers)-1}__"
    text = re.sub(r'\b\d{2,4}\s?mm\b', _cap, text, flags=re.I)
    text = re.sub(r'[$€£]\s?\d[\d.,\s]*(?:\s*(?:bn|billion|million|m|k))?', ' ', text, flags=re.I)
    text = re.sub(r'\b\d[\d.,]*\s*(?:million|billion|bn|m)\b', ' ', text, flags=re.I)
    nouns = r'(rounds?|missiles?|rockets?|drones?|uavs?|vehicles?|tanks?|ifvs?|apcs?|howitzers?|shells?|mines?|grenades?|rifles?|launchers?)'
    text = re.sub(r'(?<![A-Za-z-])\b\d[\d,.\s]*\s+(?=' + nouns + r'\b)', '', text, flags=re.I)
    text = re.sub(r'(?<![A-Za-z-])\b\d{1,3}(?:[.,]\d{3})+(?!\s?mm)\b', ' ', text)
    text = re.sub(r'(?<![A-Za-z-])\b\d{4,}(?!\s?mm)\b', ' ', text)
    for i, tok in enumerate(calibers):
        text = text.replace(f"__CAL{i}__", tok)
    text = re.sub(r'\s*;\s*', '; ', text)
    text = re.sub(r'\s{2,}', ' ', text).strip(' ;,')
    return text

def to_military_aggregated(df_raw: pd.DataFrame) -> pd.DataFrame:
    d = df_raw.copy()
    for c in ["Month","Evidence Month","Final Depreciated Value (EUR)",
              "Notable Weapons and Munitions Delivered (via PDA shipments)"]:
        if c not in d.columns:
            d[c] = np.nan
    if d.empty:
        return pd.DataFrame(columns=[
            "Month","Total Depreciated Value (approx.)",
            "Notable Weapons and Munitions Delivered (via PDA shipments)"
        ])

    d["Evidence Month"] = d["Evidence Month"].replace({"": pd.NA, "NaT": pd.NA, "nan": pd.NA})
    d["__M"] = d["Evidence Month"].where(d["Evidence Month"].notna(), d["Month"])
    val_final = pd.to_numeric(d["Final Depreciated Value (EUR)"], errors="coerce")
    val_base  = pd.to_numeric(d.get("Base Value (EUR)"), errors="coerce")
    tmp = val_final.where(val_final.notna() & (val_final > 0), other=val_base)
    d["__val"] = tmp.fillna(0.0)


    def _agg_desc(s):
        uniq = []
        for x in s:
            for part in str(x).split(";"):
                p = _clean_desc_for_agg(part)
                if p and p not in uniq:
                    uniq.append(p)
        return "; ".join(uniq[:18])

    g = d.groupby("__M", dropna=False).agg(
        total_val=("__val", "sum"),
        desc=("Notable Weapons and Munitions Delivered (via PDA shipments)", _agg_desc),
    ).reset_index().rename(columns={"__M":"Month"})

    g["__dt"] = pd.to_datetime(g["Month"], errors="coerce")
    g = g.sort_values("__dt")
    g["Month"] = g["__dt"].dt.strftime("%b %Y").str.replace(".", "", regex=False)
    g = g.drop(columns="__dt")

    g = g.rename(columns={
        "total_val": "Total Depreciated Value (approx.)",
        "desc": "Notable Weapons and Munitions Delivered (via PDA shipments)",
    })
    return g[["Month","Total Depreciated Value (approx.)",
              "Notable Weapons and Munitions Delivered (via PDA shipments)"]]

def to_loans(df: pd.DataFrame, cols: Dict[str,str], country: str) -> pd.DataFrame:
    d = df[df["bucket"]=="loans_non_military"].copy()
    rows = []
    for _, r in d.iterrows():
        title = (str(r.get(cols["type_s"], "")).strip() or str(r.get(cols["expl"], "")).strip())[:180]
        rows.append({
            "Loan Name / Instrument": title,
            "Date Issued": str(r.get("month","")),
            "Amount (EUR)": r.get("value_eur"),
            "Interest Rate": "",
            "Maturity/Repayment Terms": "",
            "Purpose & Restrictions": r.get(cols["expl"], ""),
            "Funding Source": "",
            "Capital at Risk": "",
            "Notes & Explanation": "",
            "Status": "",
            "Evidence Month": "",
            "Sources": "",
        })
    return pd.DataFrame(rows)

def to_humanitarian(df: pd.DataFrame, cols: Dict[str,str], country: str) -> pd.DataFrame:
    d = df[df["bucket"]=="direct_humanitarian_aid"].copy()
    rows = []
    for _, r in d.iterrows():
        rows.append({
            "Month": r.get("month",""),
            "Type of Aid": (str(r.get(cols["type_s"], "")) or str(r.get(cols["type_g"], ""))),
            "Detailed Description & Purpose": r.get(cols["expl"], ""),
            "Value (EUR)": r.get("value_eur"),
            "(Provider/Contractors, Beneficiary: Ukrainian Government)": "",
            "Status": "",
            "Evidence Month": "",
            "Sources": "",
        })
    return pd.DataFrame(rows)

def build_sources_to_check(df: pd.DataFrame, meta: Dict[str,str], country: str) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        for u in _search_urls_for_row(r, meta, country):
            rows.append({
                "Month": r.get("month",""),
                "Type (bucket)": r.get("bucket",""),
                "What": (str(r.get(meta["type_s"], "")) or str(r.get(meta["type_g"], "")))[:140],
                "Search URL": u
            })
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.drop_duplicates(subset=["Search URL"]).reset_index(drop=True)
    return out

# ---------- writer helpers ----------
def _col_index(df: pd.DataFrame, name: str) -> Optional[int]:
    try: return list(df.columns).index(name)
    except ValueError: return None

def _write_df_with_links(writer, sheet_name: str, df: pd.DataFrame, link_cols: List[str],
                         widths: Optional[List[int]]=None, dropdown_status: bool=False):
    safe = _safe_sheet_name(sheet_name)
    df.to_excel(writer, sheet_name=safe, index=False)
    ws = writer.sheets[safe]
    ws.freeze_panes(1,0); ws.autofilter(0,0,0, max(df.shape[1]-1,0))
    for lc in link_cols:
        ci = _col_index(df, lc)
        if ci is None: continue
        for r in range(len(df)):
            val = df.iloc[r, ci]
            if isinstance(val, str) and val.startswith("http"):
                ws.write_url(r+1, ci, val, string=val)
    if dropdown_status and "Status" in df.columns:
        c = _col_index(df, "Status")
        if c is not None and len(df)>0:
            ws.data_validation(1, c, len(df), c, {'validate':'list',
                                                  'source':['Delivered/Disbursed','Commitment/Other']})
    if "Status" in df.columns:
        c = _col_index(df, "Status")
        if c is not None and len(df)>0:
            ws.conditional_format(1, c, len(df), c,
                {'type':'text','criteria':'containing','value':'Delivered',
                 'format': writer.book.add_format({'bg_color':'#E7F6E7'})})
            ws.conditional_format(1, c, len(df), c,
                {'type':'text','criteria':'containing','value':'Commitment',
                 'format': writer.book.add_format({'bg_color':'#FDE2E2'})})
    if "Sources" in df.columns:
        c = _col_index(df, "Sources")
        if c is not None and len(df)>0:
            ws.conditional_format(1, c, len(df), c,
                {'type':'blanks','format': writer.book.add_format({'bg_color':'#FFF3BF'})})
    if widths:
        for i,wd in enumerate(widths):
            try: ws.set_column(i, i, wd)
            except: pass

def _build_qc_top_targets(mit_aggr: pd.DataFrame, loans: pd.DataFrame, hum: pd.DataFrame) -> pd.DataFrame:
    parts = []
    def prep(df, val_col, label, what_col):
        if df is None or df.empty: return None
        d = df.copy()
        d["__val"] = pd.to_numeric(d.get(val_col), errors="coerce")
        d = d.sort_values("__val", ascending=False).head(100)
        return pd.DataFrame({
            "Sheet": label,
            "Month": d.get("Month",""),
            "What": d.get(what_col,""),
            "Value": d["__val"],
        })
    a = prep(mit_aggr, "Total Depreciated Value (approx.)", "Military",
             "Notable Weapons and Munitions Delivered (via PDA shipments)")
    b = prep(loans, "Amount (EUR)", "Loans", "Loan Name / Instrument")
    c = prep(hum, "Value (EUR)", "Humanitarian", "Detailed Description & Purpose")
    for p in (a,b,c):
        if p is not None: parts.append(p)
    if not parts:
        return pd.DataFrame(columns=["Sheet","Month","What","Value"])
    out = pd.concat(parts, ignore_index=True, sort=False).sort_values("Value", ascending=False).fillna("")
    return out.head(50)

# ---------- writer (full pipeline) ----------
def write_workbook_with_enrichment(xlsx_in: str, country: str, xlsx_out: str):
    df_main = read_kiel_main(xlsx_in)
    df, meta = build_kiel_slice(df_main, country)

    mil_raw = to_military_raw(df, meta, country)

    if AID_SCRAPE_LIMIT > 0:
        mil_raw_web = _build_rows_from_web(country, limit=AID_SCRAPE_LIMIT)
        if not mil_raw_web.empty:
            mil_raw = pd.concat([mil_raw, mil_raw_web], ignore_index=True)

    mil_raw = auto_enrich_military(mil_raw, country, max_rows=AID_SCRAPE_LIMIT)

    mil_aggr = to_military_aggregated(mil_raw)
    loans = to_loans(df, meta, country)
    hum = to_humanitarian(df, meta, country)
    sources = build_sources_to_check(df, meta, country)
    qc = _build_qc_top_targets(mil_aggr, loans, hum)

    with pd.ExcelWriter(xlsx_out, engine="xlsxwriter") as w:
        _write_df_with_links(w, "Kiel data", df, link_cols=[], widths=None, dropdown_status=False)

        _write_df_with_links(
            w, "Military Raw (auto)", mil_raw,
            link_cols=["Sources"],
            widths=[12,18,110,16,12,70,14,14,14,18,18],
            dropdown_status=True
        )

        mil_us = mil_aggr[[
            "Month",
            "Total Depreciated Value (approx.)",
            "Notable Weapons and Munitions Delivered (via PDA shipments)"
        ]].copy()

        safe = _safe_sheet_name("Military Inventory Transfer")
        mil_us.to_excel(w, sheet_name=safe, index=False)
        ws = w.sheets[safe]
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0, mil_us.shape[1]-1)

        num_fmt = w.book.add_format({'num_format': '#,##0'})
        bold_fmt = w.book.add_format({'bold': True})
        ws.set_column(0, 0, 12)
        ws.set_column(1, 1, 22, num_fmt)
        ws.set_column(2, 2, 110)

        n = len(mil_us)
        total_row = n + 1
        ws.write_string(total_row, 0, "Total", bold_fmt)
        if n > 0:
            ws.write_formula(total_row, 1, f"=SUM(B2:B{n+1})", num_fmt)
        else:
            ws.write_number(total_row, 1, 0, num_fmt)

        _write_df_with_links(
            w, "Loans Non-Military", loans,
            link_cols=["Sources"],
            widths=[60,14,18,18,28,40,20,20,40,18,70],
            dropdown_status=True
        )

        _write_df_with_links(
            w, "Direct Humanitarian Aid", hum,
            link_cols=["Sources"],
            widths=[10,20,80,16,40,18,70],
            dropdown_status=True
        )

        _write_df_with_links(
            w, "Sources To Check", sources,
            link_cols=["Search URL"],
            widths=[10,16,48,90],
            dropdown_status=False
        )

        if not qc.empty:
            _write_df_with_links(
                w, "QC - Top Targets", qc,
                link_cols=[], widths=[10,12,60,16],
                dropdown_status=False
            )

    return mil_raw, mil_aggr, loans, hum, sources, qc

# ---------- interactive ----------
def donors_pick_interactive(df_main: pd.DataFrame) -> str:
    donor_col, donors = donors_list(df_main)
    print("Avaіlable countrіes:")
    for i, d in enumerate(donors[:200], 1):
        print(f"{i:>2}) {d}")
    sel = input("Choose number or pіck a country: ").strip()
    try: return donors[int(sel)-1]
    except: return sel

if __name__ == "__main__":
    print("Enter a path to Kiel Excel (fіle or folder):")
    p_raw = input("> ").strip().strip('"')
    p = Path(p_raw)
    if p.is_dir():
        cands = sorted(p.glob("*.xls*"), key=lambda x: x.stat().st_mtime, reverse=True)
        if not cands: raise SystemExit("No fіles *.xls*")
        print("Found Excel-fіle:")
        for i,f in enumerate(cands[:20],1): print(f"{i}) {f.name}")
        sel = input("Choose umber [1]: ").strip(); idx = int(sel) if sel else 1
        x_in = str(cands[idx-1].resolve())
    else:
        x_in = str(p.resolve())

    df_main = read_kiel_main(x_in)
    country = donors_pick_interactive(df_main)

    out_name = re.sub(r"[^A-Za-z0-9]+","_", country).lower() + "_compiled.xlsx"
    print("Workіng")
    mil_raw, mil_aggr, loans, hum, sources, qc = write_workbook_with_enrichment(x_in, country, out_name)
    print("Done", out_name)