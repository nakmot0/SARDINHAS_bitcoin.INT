import os
import re
import json
import time
import bisect
import sqlite3
import hashlib
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timedelta

import requests
import feedparser
import yfinance as _yf
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "data.json"
HTML_FILE = BASE_DIR / "INDEX.HTML"
SOURCES_FILE = BASE_DIR / "sources.txt"

# BD persistente no disco do Render (/data) ou local
_disk = os.environ.get("RENDER_DISK_PATH", str(BASE_DIR))
DB_PATH = Path(_disk) / "memory.db"

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.1-8b-instant"  # rápido + poupa tokens free tier
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")


# ── BASE DE DADOS — memória do agente ─────────────────────────────────────────
def db_init():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS conversations (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role       TEXT NOT NULL,
            content    TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sess ON conversations(session_id);
        CREATE TABLE IF NOT EXISTS common_questions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            question  TEXT NOT NULL UNIQUE,
            count     INTEGER DEFAULT 1,
            last_seen TEXT NOT NULL
        );
    """)
    con.commit()
    con.close()
    logger.info(f"DB: {DB_PATH}")


def db_save(sid, role, content):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO conversations (session_id,role,content,created_at) VALUES (?,?,?,?)",
            (sid, role, content, datetime.now().isoformat()),
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"db_save: {e}")


def db_history(sid, limit=10):
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            "SELECT role,content FROM conversations WHERE session_id=? ORDER BY id DESC LIMIT ?",
            (sid, limit),
        ).fetchall()
        con.close()
        return [{"role": r, "content": c} for r, c in reversed(rows)]
    except Exception:
        return []


def db_track(q):
    qn = q.strip().lower()[:200]
    try:
        con = sqlite3.connect(DB_PATH)
        try:
            con.execute(
                "INSERT INTO common_questions (question,count,last_seen) VALUES (?,1,?) "
                "ON CONFLICT(question) DO UPDATE SET count=count+1,last_seen=excluded.last_seen",
                (qn, datetime.now().isoformat()),
            )
        except Exception:
            con.execute(
                "UPDATE common_questions SET count=count+1,last_seen=? WHERE question=?",
                (datetime.now().isoformat(), qn),
            )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"db_track: {e}")


def db_top(n=5):
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            "SELECT question,count FROM common_questions ORDER BY count DESC LIMIT ?",
            (n,),
        ).fetchall()
        con.close()
        return rows
    except Exception:
        return []


def db_stats():
    try:
        con = sqlite3.connect(DB_PATH)
        msgs = con.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
        sess = con.execute(
            "SELECT COUNT(DISTINCT session_id) FROM conversations"
        ).fetchone()[0]
        qs = con.execute("SELECT COUNT(*) FROM common_questions").fetchone()[0]
        con.close()
        return {"messages": msgs, "sessions": sess, "unique_questions": qs}
    except Exception:
        return {"messages": 0, "sessions": 0, "unique_questions": 0}


# ── CACHE em memória ──────────────────────────────────────────────────────────
_cache = {}
CACHE_TTL = {
    "prices": 60,
    "news": 600,  # notícias: 10 min (evita sobrecarga RSS)
    "feargreed": 3600,
    "agent_summary": 900,  # 15 min
    "btc_rank": 1800,  # rank global: 30 min
}


def cache_get(key):
    e = _cache.get(key)
    if e and (time.time() - e["ts"]) < CACHE_TTL.get(key, 60):
        return e["data"]
    return None


def cache_set(key, data):
    _cache[key] = {"data": data, "ts": time.time()}


# ── HELPER data.json ──────────────────────────────────────────────────────────
def get_dashboard_data():
    try:
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {
            "bitcoin": {"price": "Carregando...", "change": "n/d", "dominance": "n/d"},
            "eth": {"btc": "n/d"},
            "gold": {"btc": "n/d"},
            "dxy": {"state": "n/d"},
            "editorial": "Aguardando primeira análise...",
            "updated": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }


# ── BITCOIN GLOBAL RANK ───────────────────────────────────────────────────────
# Compara market cap BTC vs ouro + prata + top empresas
# (yf_ticker, stooq_ticker, shares_outstanding_aprox, fx)
# stooq_ticker=None → sem fallback Stooq (ex: Saudi Aramco)
_RANK_COMPANIES = [
    ("NVDA",    "nvda.us",   24_400_000_000, 1.0),
    ("AAPL",    "aapl.us",   15_200_000_000, 1.0),
    ("MSFT",    "msft.us",    7_440_000_000, 1.0),
    ("GOOGL",   "googl.us",  12_100_000_000, 1.0),
    ("AMZN",    "amzn.us",   10_500_000_000, 1.0),
    ("META",    "meta.us",    2_540_000_000, 1.0),
    ("AVGO",    "avgo.us",    4_770_000_000, 1.0),
    ("TSM",     "tsm.us",     5_181_000_000, 1.0),   # ADR (1 ADR = 5 ações TSMC)
    ("TSLA",    "tsla.us",    3_210_000_000, 1.0),
    ("BRK-B",   "brk-b.us",  2_160_000_000, 1.0),
    ("LLY",     "lly.us",      896_000_000, 1.0),
    ("JPM",     "jpm.us",    2_810_000_000, 1.0),
    ("WMT",     "wmt.us",    8_060_000_000, 1.0),
    ("V",       "v.us",      2_060_000_000, 1.0),
    ("XOM",     "xom.us",    3_970_000_000, 1.0),
    ("2222.SR", None,      237_500_000_000, 1/3.75),  # Saudi Aramco: SAR→USD, sem Stooq
]


def _get_mc(yf_ticker, stooq_ticker, shares, fx):
    """Market cap em USD: Yahoo Finance primário, Stooq fallback."""
    # 1) Yahoo Finance
    try:
        _c = _get_yf_crumb()
        r = _yf_session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_ticker}?interval=1d&range=1d"
            + (f"&crumb={_c}" if _c else ""),
            timeout=8,
        )
        r.raise_for_status()
        closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes and closes[-1] > 0:
            return closes[-1] * fx * shares
    except Exception:
        pass
    # 2) Stooq fallback (apenas ações US; Saudi Aramco não tem ticker Stooq)
    if stooq_ticker:
        try:
            r2 = requests.get(
                f"https://stooq.com/q/l/?s={stooq_ticker}&f=sd2t2ohlcv&h&e=csv",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            r2.raise_for_status()
            lines = [l.strip() for l in r2.text.strip().split("\n") if l.strip()][1:]
            for line in reversed(lines):
                cols = line.split(",")
                if len(cols) >= 5:
                    try:
                        price = float(cols[4])  # close
                        if price > 0:
                            logger.info(f"Rank MC {yf_ticker} via Stooq: ${price:.2f}")
                            return price * fx * shares
                    except Exception:
                        continue
        except Exception as e:
            logger.warning(f"Stooq MC {stooq_ticker}: {e}")
    return 0


def fetch_btc_rank(btc_usd, gold_usd=None, silver_usd=None):
    """Devolve a posição global do BTC por market cap."""
    btc_mc = btc_usd * 19_870_000  # ~19.87M coins em circulação (Março 2026)
    market_caps = [btc_mc]
    if gold_usd:
        market_caps.append(gold_usd * 6_915_000_000)   # ~6.9B troy oz acima do solo
    if silver_usd:
        market_caps.append(silver_usd * 55_900_000_000)  # ~55.9B troy oz acima do solo

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_get_mc, t, st, s, fx) for t, st, s, fx in _RANK_COMPANIES]
        for f in as_completed(futures):
            mc = f.result()
            if mc > 0:
                market_caps.append(mc)

    ranked = sorted(market_caps, reverse=True)
    logger.info(f"BTC rank: {len(market_caps)} assets comparados, MC BTC=${btc_mc/1e12:.2f}T")
    try:
        return ranked.index(btc_mc) + 1
    except ValueError:
        return None


# ── FETCH PREÇOS (Kraken + Coinpaprika + Stooq) ───────────────────────────────
def stooq_val(ticker, lo, hi):
    """Busca valor numérico do Stooq — funciona em cloud sem bloqueio."""
    r = requests.get(
        f"https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=12,
    )
    r.raise_for_status()
    for line in reversed(
        [l.strip() for l in r.text.strip().split("\n") if l.strip()][1:]
    ):
        for col in reversed(line.split(",")):
            try:
                v = float(col)
                if lo < v < hi:
                    return v
            except Exception:
                continue
    return None


def fetch_prices():
    result = {
        "btc_usd": None,
        "btc_change_24h": None,
        "btc_change_1h": None,
        "eth_btc": None,
        "gold_btc": None,
        "dominance": None,
        "crude_usd": None,
        "crude_btc": None,
        "silver_usd": None,
        "silver_btc": None,
        "nasdaq_usd": None,
        "nasdaq_btc": None,
        "btc_rank": None,
        "source": [],
    }
    H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

    # ── Kraken: BTC spot + ETH/BTC ───────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHXBT",
            headers=H,
            timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("result", {})
        for k in ["XXBTZUSD", "XBTUSD"]:
            if k in d:
                result["btc_usd"] = float(d[k]["c"][0])
                break
        for k in ["XETHXXBT", "ETHXBT"]:
            if k in d:
                result["eth_btc"] = float(d[k]["c"][0])
                break
        result["source"].append("Kraken")
        logger.info(f"BTC: ${result['btc_usd']:,.0f} | ETH/BTC: {result['eth_btc']}")
    except Exception as e:
        logger.warning(f"Kraken ticker: {e}")

    # Fallback BTC: Coinbase
    if result["btc_usd"] is None:
        try:
            r = requests.get(
                "https://api.coinbase.com/v2/prices/BTC-USD/spot", headers=H, timeout=8
            )
            r.raise_for_status()
            result["btc_usd"] = float(r.json()["data"]["amount"])
            result["source"].append("Coinbase")
        except Exception as e:
            logger.warning(f"Coinbase: {e}")

    # ── Kraken OHLC: variação 24h ─────────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440",
            headers=H,
            timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("result", {})
        key = next((k for k in d if k != "last"), None)
        if key and len(d[key]) >= 2:
            o, c = float(d[key][-2][1]), float(d[key][-1][4])
            if o > 0:
                result["btc_change_24h"] = round((c - o) / o * 100, 3)
    except Exception as e:
        logger.warning(f"Kraken 24h: {e}")

    # ── Kraken OHLC: variação 1h ──────────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60",
            headers=H,
            timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("result", {})
        key = next((k for k in d if k != "last"), None)
        if key and len(d[key]) >= 2:
            o, c = float(d[key][-2][1]), float(d[key][-1][4])
            if o > 0:
                result["btc_change_1h"] = round((c - o) / o * 100, 3)
    except Exception as e:
        logger.warning(f"Kraken 1h: {e}")

    # ── Dominância: Coinpaprika (sem rate-limit em cloud) ────────────────────
    try:
        r = requests.get("https://api.coinpaprika.com/v1/global", headers=H, timeout=10)
        r.raise_for_status()
        result["dominance"] = r.json().get("bitcoin_dominance_percentage")
    except Exception as e:
        logger.warning(f"Coinpaprika: {e}")

    # ── Ouro: CoinGecko tether-gold (XAUT) ───────────────────────────────────
    # XAUT é 1 troy oz de ouro tokenizado — preço = preço real do ouro
    gold_usd = None
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=tether-gold&vs_currencies=usd",
            headers=H,
            timeout=12,
        )
        r.raise_for_status()
        gold_usd = r.json().get("tether-gold", {}).get("usd")
        if gold_usd:
            logger.info(f"Ouro (CoinGecko XAUT): ${gold_usd:.0f}")
    except Exception as e:
        logger.warning(f"CoinGecko XAUT: {e}")

    # Fallback ouro: Coinpaprika XAU-gold
    if not gold_usd:
        try:
            r = requests.get(
                "https://api.coinpaprika.com/v1/tickers/xaut-tether-gold",
                headers=H,
                timeout=10,
            )
            r.raise_for_status()
            gold_usd = r.json().get("quotes", {}).get("USD", {}).get("price")
            if gold_usd:
                logger.info(f"Ouro (Coinpaprika XAUT): ${gold_usd:.0f}")
        except Exception as e:
            logger.warning(f"Coinpaprika XAUT: {e}")

    if gold_usd and result["btc_usd"]:
        result["gold_btc"] = round(float(gold_usd) / result["btc_usd"], 6)
        result["gold_usd"] = round(float(gold_usd), 2)

    # ── ETH/USD: Binance ─────────────────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT",
            headers=H,
            timeout=8,
        )
        r.raise_for_status()
        eth_usd = float(r.json().get("price", 0))
        if eth_usd > 0:
            result["eth_usd"] = round(eth_usd, 2)
            logger.info(f"ETH: ${eth_usd:.2f}")
    except Exception as e:
        logger.warning(f"Binance ETH: {e}")
        # Fallback: calcular de btc_usd * eth_btc
        if result.get("btc_usd") and result.get("eth_btc"):
            result["eth_usd"] = round(result["btc_usd"] * result["eth_btc"], 2)

    # ── Petróleo WTI: yfinance → Stooq → EIA → cache ────────────────────────
    crude_usd = _yf_fast_price("CL=F", 40, 200)
    if crude_usd:
        logger.info(f"WTI (yfinance): ${crude_usd:.1f}")
    if not crude_usd:
        _yf_c = _get_yf_crumb()
        try:
            ry = _yf_session.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/CL=F"
                "?interval=1d&range=5d" + (f"&crumb={_yf_c}" if _yf_c else ""),
                timeout=10,
            )
            ry.raise_for_status()
            closes = ry.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes = [c for c in closes if c is not None]
            if closes:
                crude_usd = round(closes[-1], 2)
                logger.info(f"WTI (Yahoo raw): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"Yahoo WTI raw: {e}")
    if not crude_usd:
        crude_usd = stooq_val("cl.f", 40, 200)
        if crude_usd:
            logger.info(f"WTI (Stooq): ${crude_usd:.1f}")
    if not crude_usd:
        try:
            r = requests.get(
                "https://api.eia.gov/v2/petroleum/pri/spt/data/"
                "?api_key=DEMO_KEY&frequency=daily&data[0]=value"
                "&facets[series][]=RWTC&sort[0][column]=period"
                "&sort[0][direction]=desc&length=1",
                headers=H, timeout=8,
            )
            r.raise_for_status()
            rows = r.json().get("response", {}).get("data", [])
            if rows:
                crude_usd = float(rows[0]["value"])
                logger.info(f"WTI (EIA): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"EIA WTI: {e}")
    if not crude_usd:
        cached_prices = cache_get("prices")
        if cached_prices and cached_prices.get("crude_usd"):
            crude_usd = cached_prices["crude_usd"]
            logger.info(f"WTI (cache): ${crude_usd:.1f}")

    if crude_usd and result["btc_usd"]:
        result["crude_usd"] = crude_usd
        result["crude_btc"] = round(crude_usd / result["btc_usd"], 6)

    # ── S&P 500: yfinance → Yahoo raw → Stooq ────────────────────────────────
    sp500_usd = _yf_fast_price("^GSPC", 3000, 7000)
    if sp500_usd:
        logger.info(f"SP500 (yfinance): ${sp500_usd:,.0f}")
    if not sp500_usd:
        _yf_c = _get_yf_crumb()
        try:
            ry = _yf_session.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC"
                "?interval=1d&range=5d" + (f"&crumb={_yf_c}" if _yf_c else ""),
                timeout=10,
            )
            ry.raise_for_status()
            sp_closes = ry.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            sp_closes = [c for c in sp_closes if c is not None]
            if sp_closes:
                sp500_usd = round(sp_closes[-1], 2)
                logger.info(f"SP500 (Yahoo raw): ${sp500_usd:,.0f}")
        except Exception as e:
            logger.warning(f"Yahoo SP500 raw: {e}")
    if not sp500_usd:
        sp500_usd = stooq_val("^spx", 3000, 7000)
        if sp500_usd:
            logger.info(f"SP500 (Stooq): ${sp500_usd:,.0f}")
    if sp500_usd and result["btc_usd"]:
        result["sp500_usd"] = round(sp500_usd, 2)
        result["sp500_btc"] = round(sp500_usd / result["btc_usd"], 6)

    # ── Prata: yfinance → Yahoo raw → Stooq ──────────────────────────────────
    silver_usd = _yf_fast_price("SI=F", 5, 200)
    if silver_usd:
        logger.info(f"Prata (yfinance): ${silver_usd:.2f}")
    if not silver_usd:
        _yf_c = _get_yf_crumb()
        try:
            ry = _yf_session.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/SI=F"
                "?interval=1d&range=5d" + (f"&crumb={_yf_c}" if _yf_c else ""),
                timeout=10,
            )
            ry.raise_for_status()
            ag_closes = ry.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            ag_closes = [c for c in ag_closes if c is not None]
            if ag_closes:
                silver_usd = round(ag_closes[-1], 4)
                logger.info(f"Prata (Yahoo raw): ${silver_usd:.2f}")
        except Exception as e:
            logger.warning(f"Yahoo Silver raw: {e}")
    if not silver_usd:
        silver_usd = stooq_val("si.f", 5, 200)
        if silver_usd:
            logger.info(f"Prata (Stooq): ${silver_usd:.2f}")
    if silver_usd and result["btc_usd"]:
        result["silver_usd"] = round(float(silver_usd), 4)
        result["silver_btc"] = round(float(silver_usd) / result["btc_usd"], 8)

    # ── NASDAQ: yfinance → Yahoo raw → Stooq ─────────────────────────────────
    nasdaq_usd = _yf_fast_price("^IXIC", 8000, 30000)
    if nasdaq_usd:
        logger.info(f"NASDAQ (yfinance): ${nasdaq_usd:,.0f}")
    if not nasdaq_usd:
        _yf_c = _get_yf_crumb()
        try:
            ry = _yf_session.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC"
                "?interval=1d&range=5d" + (f"&crumb={_yf_c}" if _yf_c else ""),
                timeout=10,
            )
            ry.raise_for_status()
            nq_closes = ry.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            nq_closes = [c for c in nq_closes if c is not None]
            if nq_closes:
                nasdaq_usd = round(nq_closes[-1], 2)
                logger.info(f"NASDAQ (Yahoo raw): ${nasdaq_usd:,.0f}")
        except Exception as e:
            logger.warning(f"Yahoo NASDAQ raw: {e}")
    if not nasdaq_usd:
        nasdaq_usd = stooq_val("^ndq", 8000, 30000)
        if nasdaq_usd:
            logger.info(f"NASDAQ (Stooq): ${nasdaq_usd:,.0f}")
    if nasdaq_usd and result["btc_usd"]:
        result["nasdaq_usd"] = round(nasdaq_usd, 2)
        result["nasdaq_btc"] = round(nasdaq_usd / result["btc_usd"], 6)

    # ── Bitcoin Global Rank (cache 30 min) ───────────────────────────────────
    cached_rank = cache_get("btc_rank")
    if cached_rank is not None:
        result["btc_rank"] = cached_rank
    elif result["btc_usd"]:
        try:
            rank = fetch_btc_rank(result["btc_usd"], result.get("gold_usd"), result.get("silver_usd"))
            if rank:
                result["btc_rank"] = rank
                cache_set("btc_rank", rank)
                logger.info(f"BTC global rank calculado: #{rank}")
        except Exception as e:
            logger.warning(f"BTC rank: {e}")

    result["updated"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return result


# ── NOTÍCIAS ──────────────────────────────────────────────────────────────────
BULLISH = [
    "bull",
    "surge",
    "rally",
    "high",
    "gain",
    "up",
    "rise",
    "record",
    "ath",
    "buy",
    "halving",
    "adoption",
    "etf",
    "inflow",
    "alta",
    "subida",
    "recorde",
    "compra",
    "adoção",
    "topo",
    "crescimento",
    "valoriza",
    "positivo",
    "máximo",
    "acumula",
    "supera",
]
BEARISH = [
    "bear",
    "crash",
    "drop",
    "down",
    "fall",
    "low",
    "sell",
    "dump",
    "fear",
    "hack",
    "ban",
    "risk",
    "outflow",
    "decline",
    "queda",
    "baixa",
    "venda",
    "pânico",
    "proibição",
    "colapso",
    "risco",
    "negativo",
    "mínimo",
    "cai",
    "perda",
    "recua",
]


def classify(title):
    t = title.lower()
    b = sum(1 for w in BULLISH if w in t)
    n = sum(1 for w in BEARISH if w in t)
    return "bullish" if b > n else "bearish" if n > b else "neutral"


def load_sources():
    if not SOURCES_FILE.exists():
        return [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
        ]
    sources = []
    for line in SOURCES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Ignorar instâncias Nitter (frequentemente offline)
        if "nitter" in line:
            continue
        sources.append(line)
    return sources


def fetch_news():
    H = {"User-Agent": "Mozilla/5.0 (compatible; BitcoinIntelligence/1.0)"}
    sources = load_sources()
    items = []
    for url in sources[:8]:
        if len(items) >= 20:
            break
        try:
            resp = requests.get(url, headers=H, timeout=8)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            name = (
                feed.feed.get("title", url.split("/")[2])
                if feed.feed
                else url.split("/")[2]
            )
            for entry in feed.entries[:3]:
                title = entry.get("title", "").strip()
                if not title:
                    continue
                pub = entry.get("published", "")
                date_str = pub[:16] if pub else "--"
                try:
                    import email.utils

                    dt = email.utils.parsedate_to_datetime(pub)
                    date_str = dt.strftime("%d %b %H:%M")
                except Exception:
                    pass
                items.append(
                    {
                        "title": title,
                        "source": name,
                        "date": date_str,
                        "sentiment": classify(title),
                        "link": entry.get("link", ""),
                    }
                )
        except Exception as e:
            logger.warning(f"Feed {url}: {e}")

    if not items:
        items = [
            {
                "title": "Sem notícias disponíveis",
                "source": "Sistema",
                "date": "--",
                "sentiment": "neutral",
                "link": "",
            }
        ]
    return {"items": items, "updated": datetime.now().strftime("%H:%M")}


# ── GROQ ──────────────────────────────────────────────────────────────────────
def call_groq(system_prompt, messages, max_tokens=500):
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY não configurada no Render → Environment.")
    payload = [{"role": "system", "content": system_prompt}]
    for m in messages:
        payload.append(
            {
                "role": "user" if m["role"] == "user" else "assistant",
                "content": m["content"],
            }
        )
    resp = requests.post(
        GROQ_API_URL,
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": GROQ_MODEL,
            "messages": payload,
            "max_tokens": max_tokens,
            "temperature": 0.4,
        },
        timeout=30,
    )
    if resp.status_code == 401:
        raise RuntimeError("GROQ_API_KEY inválida.")
    if resp.status_code == 429:
        raise RuntimeError("Limite Groq atingido (25k tokens/dia). Tenta amanhã.")
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def build_system(snapshot=None, lang="pt"):
    now = datetime.now().strftime("%A, %d %B %Y %H:%M")
    en = lang == "en"
    if snapshot:
        s = snapshot
        data_block = (
            f"Real-time data:\n"
            f"- BTC/USD: {s.get('price', '--')} (1h: {s.get('change1h', '--')} | 24h: {s.get('change', '--')})\n"
            f"- Dominance: {s.get('dom', '--')} | Fear&Greed: {s.get('fg', '--')} {s.get('fgLabel', '')}\n"
            f"- ETH/BTC: {s.get('ethBtc', '--')} | Gold/BTC: {s.get('goldBtc', '--')} | WTI/BTC: {s.get('crudeBtc', '--')}\n"
            f"- Post-halving cycle: {s.get('halvDays', '--')} ({s.get('halvPct', '--')})"
            if en else
            f"Dados em tempo real:\n"
            f"- BTC/USD: {s.get('price', '--')} (1h: {s.get('change1h', '--')} | 24h: {s.get('change', '--')})\n"
            f"- Dominância: {s.get('dom', '--')} | Fear&Greed: {s.get('fg', '--')} {s.get('fgLabel', '')}\n"
            f"- ETH/BTC: {s.get('ethBtc', '--')} | Ouro/BTC: {s.get('goldBtc', '--')} | WTI/BTC: {s.get('crudeBtc', '--')}\n"
            f"- Ciclo pós-halving: {s.get('halvDays', '--')} ({s.get('halvPct', '--')})"
        )
    else:
        p = cache_get("prices") or {}
        fg = cache_get("feargreed") or {}
        ch24 = p.get("btc_change_24h")
        ch1h = p.get("btc_change_1h")
        cr = p.get("crude_btc")
        dom = p.get("dominance")
        data_block = (
            f"Real-time data:\n"
            f"- BTC/USD: ${p.get('btc_usd', '--')} "
            f"(1h: {('%.2f%%' % ch1h) if ch1h is not None else '--'} | "
            f"24h: {('%.2f%%' % ch24) if ch24 is not None else '--'})\n"
            f"- Dominance: {('%.1f%%' % dom) if dom is not None else '--'} | "
            f"Fear&Greed: {fg.get('value', '--')} {fg.get('classification', '')}\n"
            f"- ETH/BTC: {p.get('eth_btc', '--')} | Gold/BTC: {p.get('gold_btc', '--')} | "
            f"WTI/BTC: {('%.5f BTC' % cr) if cr is not None else '--'}"
            if en else
            f"Dados em tempo real:\n"
            f"- BTC/USD: ${p.get('btc_usd', '--')} "
            f"(1h: {('%.2f%%' % ch1h) if ch1h is not None else '--'} | "
            f"24h: {('%.2f%%' % ch24) if ch24 is not None else '--'})\n"
            f"- Dominância: {('%.1f%%' % dom) if dom is not None else '--'} | "
            f"Fear&Greed: {fg.get('value', '--')} {fg.get('classification', '')}\n"
            f"- ETH/BTC: {p.get('eth_btc', '--')} | Ouro/BTC: {p.get('gold_btc', '--')} | "
            f"WTI/BTC: {('%.5f BTC' % cr) if cr is not None else '--'}"
        )
    top_q = db_top(3)
    mem = (
        ("\nMost asked topics: " if en else "\nTópicos mais perguntados: ")
        + " | ".join(f'"{q}"({c}x)' for q, c in top_q)
        if top_q else ""
    )
    if en:
        return (
            "You are a specialist agent in financial market analysis focused on Bitcoin. "
            "You ALWAYS respond in English, clearly and directly.\n\n"
            f"Date: {now}\n\n{data_block}{mem}\n\n"
            "Be concise (3-5 sentences). No emojis. Nothing is direct financial advice."
        )
    return (
        "És um agente especializado em análise de mercados financeiros com foco em Bitcoin. "
        "Respondes SEMPRE em português de Portugal (PT-PT), de forma clara e directa.\n\n"
        f"Data: {now}\n\n{data_block}{mem}\n\n"
        "Sê conciso (3-5 frases). Sem emojis. Nada é conselho financeiro directo."
    )


# ── FEAR & GREED ──────────────────────────────────────────────────────────────
def fetch_feargreed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=7", timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            return {"value": "50", "yesterday": "50", "last_week": "50"}
        return {
            "value": data[0]["value"],
            "classification": data[0].get("value_classification", ""),
            "yesterday": data[1]["value"] if len(data) > 1 else data[0]["value"],
            "last_week": data[6]["value"] if len(data) > 6 else data[0]["value"],
        }
    except Exception as e:
        logger.warning(f"FearGreed: {e}")
        return {"value": "50", "yesterday": "50", "last_week": "50"}


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.after_request
def no_cache(response):
    """Evitar que o browser cacheie páginas e APIs — dados devem ser sempre frescos."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/")
def index():
    try:
        return (
            HTML_FILE.read_text(encoding="utf-8"),
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )
    except FileNotFoundError:
        return "<h1>INDEX.HTML não encontrado</h1>", 404


@app.route("/data.json")
def data_json():
    if not DATA_FILE.exists():
        return jsonify({"error": "data.json não encontrado"}), 404
    return app.response_class(
        DATA_FILE.read_text(encoding="utf-8"), mimetype="application/json"
    )


@app.route("/sw.js")
def service_worker():
    return send_from_directory(
        str(BASE_DIR), "sw.js", mimetype="application/javascript"
    )


@app.route("/manifest.json")
def manifest():
    return send_from_directory(
        str(BASE_DIR), "manifest.json", mimetype="application/manifest+json"
    )


@app.route("/icons/<path:filename>")
def icons(filename):
    return send_from_directory(str(BASE_DIR / "icons"), filename)


@app.route("/api/data")
def api_data():
    return jsonify(get_dashboard_data()), 200


@app.route("/api/prices")
def api_prices():
    data = cache_get("prices")
    if data is None:
        data = fetch_prices()
        cache_set("prices", data)
    return jsonify(data), 200


@app.route("/api/feargreed")
def api_feargreed():
    data = cache_get("feargreed")
    if data is None:
        data = fetch_feargreed()
        cache_set("feargreed", data)
    return jsonify(data), 200


@app.route("/api/news")
def api_news():
    data = cache_get("news")
    if data is None:
        data = fetch_news()
        cache_set("news", data)
    return jsonify(data), 200


@app.route("/api/btc-tick")
def api_btc_tick():
    """Ticker rápido BTC — usa cache de /api/prices (sem chamada extra)."""
    cached = cache_get("prices")
    if cached and cached.get("btc_usd"):
        return jsonify(
            {
                "btc_usd": cached["btc_usd"],
                "btc_change_24h": cached.get("btc_change_24h"),
                "btc_change_1h": cached.get("btc_change_1h"),
                "from_cache": True,
            }
        ), 200
    # Cache vazia — buscar só o preço spot rapidamente
    try:
        r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=5)
        r.raise_for_status()
        return jsonify(
            {"btc_usd": float(r.json()["data"]["amount"]), "btc_change_24h": None}
        ), 200
    except Exception:
        return jsonify({"error": "unavailable"}), 502


def build_smart_summary(lang="pt"):
    """Análise automática inteligente baseada em templates — sem API externa."""
    p = cache_get("prices") or {}
    fg = cache_get("feargreed") or {}
    news_data = cache_get("news") or {}

    btc = p.get("btc_usd")
    ch24 = p.get("btc_change_24h")
    dom = p.get("dominance")
    fg_val = int(fg.get("value", 50))
    fg_label = fg.get("classification", "")

    cycle_cache = _ohlc_cache.get("cycle_stats", {}).get("data", {})
    cycle_high = cycle_cache.get("cycle_high")
    cycle_low  = cycle_cache.get("cycle_low")
    cycle_high_date = cycle_cache.get("cycle_high_date", "")
    cycle_low_date  = cycle_cache.get("cycle_low_date", "")

    halving_date = datetime(2024, 4, 19)
    cycle_day = (datetime.now() - halving_date).days
    cycle_pct = round(cycle_day / 1461 * 100, 1)

    headlines = news_data.get("items", [])[:8]
    bullish_n = sum(1 for n in headlines if n.get("sentiment") == "bullish")
    bearish_n = sum(1 for n in headlines if n.get("sentiment") == "bearish")
    neutral_n = len(headlines) - bullish_n - bearish_n

    en = lang == "en"

    # ── Linha de preço ─────────────────────────────────────────────────────────
    if btc and ch24 is not None:
        btc_s = f"${btc:,.0f}"
        if ch24 > 3:
            price_line = (
                f"Bitcoin is up {ch24:.1f}% in the last 24h, trading at {btc_s}, showing strong bullish momentum."
                if en else
                f"O Bitcoin valoriza {ch24:.1f}% nas últimas 24h e situa-se em {btc_s}, demonstrando momentum ascendente relevante."
            )
        elif ch24 > 0.5:
            price_line = (
                f"Bitcoin is holding at {btc_s} with a moderate gain of {ch24:.1f}% over the last 24h."
                if en else
                f"O Bitcoin mantém-se em {btc_s} com uma subida moderada de {ch24:.1f}% nas últimas 24h."
            )
        elif ch24 < -3:
            price_line = (
                f"Bitcoin is down {abs(ch24):.1f}% in the last 24h to {btc_s}, signalling significant selling pressure."
                if en else
                f"O Bitcoin recua {abs(ch24):.1f}% nas últimas 24h para {btc_s}, sinalizando pressão vendedora significativa."
            )
        elif ch24 < -0.5:
            price_line = (
                f"Bitcoin is giving back ground to {btc_s} with a {abs(ch24):.1f}% decline over 24h."
                if en else
                f"O Bitcoin cede terreno para {btc_s} com uma queda de {abs(ch24):.1f}% nas últimas 24h."
            )
        else:
            price_line = (
                f"Bitcoin is consolidating around {btc_s}, with a minimal {ch24:+.1f}% change over 24h."
                if en else
                f"O Bitcoin consolida em torno de {btc_s}, com variação mínima de {ch24:+.1f}% nas últimas 24h."
            )
    else:
        price_line = "Price data currently unavailable." if en else "Dados de preço indisponíveis de momento."

    # ── Linha de notícias ──────────────────────────────────────────────────────
    if headlines:
        if bullish_n > bearish_n + 1:
            news_line = (
                f"News flow is predominantly positive ({bullish_n} bullish vs {bearish_n} bearish), driven by institutional adoption and favourable technical catalysts."
                if en else
                f"O fluxo noticioso é predominantemente positivo ({bullish_n} bullish vs {bearish_n} bearish), com destaques para adopção institucional e catalisadores técnicos favoráveis."
            )
        elif bearish_n > bullish_n + 1:
            news_line = (
                f"News is leaning negative ({bearish_n} bearish vs {bullish_n} bullish), reflecting regulatory uncertainty or macro headwinds."
                if en else
                f"As notícias inclinam-se para o lado negativo ({bearish_n} bearish vs {bullish_n} bullish), reflectindo incerteza regulatória ou pressão macro."
            )
        else:
            news_line = (
                f"News flow is mixed ({bullish_n} bullish, {bearish_n} bearish, {neutral_n} neutral), with no dominant short-term catalyst."
                if en else
                f"O fluxo noticioso é misto ({bullish_n} bullish, {bearish_n} bearish, {neutral_n} neutras), sem catalisador dominante a curto prazo."
            )
    else:
        news_line = (
            "No recent news available to contextualise the move."
            if en else
            "Sem notícias recentes disponíveis para contextualizar o movimento."
        )

    # ── Linha Fear & Greed ─────────────────────────────────────────────────────
    if fg_val >= 75:
        fg_line = (
            f"Fear & Greed at {fg_val} ({fg_label}) signals extreme greed — historically associated with elevated correction risk."
            if en else
            f"O Fear & Greed em {fg_val} ({fg_label}) sinaliza ganância extrema — território historicamente associado a maior risco de correcção."
        )
    elif fg_val >= 55:
        fg_line = (
            f"Market sentiment is in greed territory ({fg_val}), with participants receptive to new positions."
            if en else
            f"O sentimento de mercado está em zona de ganância ({fg_val}), com o mercado receptivo a novas posições."
        )
    elif fg_val >= 45:
        fg_line = (
            f"Fear & Greed at {fg_val} reflects a neutral market, waiting for a directional catalyst."
            if en else
            f"O Fear & Greed em {fg_val} reflecte um mercado neutro, à espera de catalisador de direcção."
        )
    elif fg_val >= 25:
        fg_line = (
            f"With Fear & Greed at {fg_val} (fear), sentiment is pessimistic — historically a long-term accumulation zone."
            if en else
            f"Com o Fear & Greed em {fg_val} (medo), o sentimento é pessimista — historicamente zona de acumulação para investidores de longo prazo."
        )
    else:
        fg_line = (
            f"Fear & Greed at {fg_val} ({fg_label}) signals extreme fear — possible capitulation but also a historical accumulation opportunity."
            if en else
            f"O índice Fear & Greed em {fg_val} ({fg_label}) sinaliza medo extremo, com possível capitulação mas também oportunidade histórica de acumulação."
        )

    # ── Linha dominância ───────────────────────────────────────────────────────
    if dom and dom > 0:
        if dom > 58:
            dom_line = (
                f"Bitcoin dominance at {dom:.1f}% signals flight-to-quality — crypto capital is concentrating in BTC."
                if en else
                f"A dominância do Bitcoin em {dom:.1f}% indica flight-to-quality — o capital cripto concentra-se em BTC."
            )
        elif dom < 48:
            dom_line = (
                f"With dominance at {dom:.1f}%, the market is in risk-on mode, with capital flowing into altcoins."
                if en else
                f"Com dominância em {dom:.1f}%, o mercado está em modo risk-on, com capital a fluir para altcoins."
            )
        else:
            dom_line = (
                f"Dominance at {dom:.1f}% positions Bitcoin as the stable core of the crypto market."
                if en else
                f"A dominância de {dom:.1f}% posiciona o Bitcoin como núcleo estável do mercado cripto."
            )
    else:
        dom_line = ""

    # ── Máximo / Mínimo do ciclo ────────────────────────────────────────────────
    if cycle_high and cycle_low and btc:
        pct_from_high = round((btc - cycle_high) / cycle_high * 100, 1)
        pct_from_low  = round((btc - cycle_low)  / cycle_low  * 100, 1)
        if pct_from_high >= -5:
            cycle_stats_line = (
                f"Price is near the cycle high of ${cycle_high:,.0f} ({cycle_high_date}), only {abs(pct_from_high):.1f}% below the peak."
                if en else
                f"O preço está próximo do máximo do ciclo de ${cycle_high:,.0f} ({cycle_high_date}), apenas {abs(pct_from_high):.1f}% abaixo do pico."
            )
        elif pct_from_high < -30:
            cycle_stats_line = (
                f"Price is {abs(pct_from_high):.0f}% below the cycle high (${cycle_high:,.0f}, {cycle_high_date}) — currently in drawdown, +{pct_from_low:.0f}% above cycle low."
                if en else
                f"O preço está {abs(pct_from_high):.0f}% abaixo do máximo do ciclo (${cycle_high:,.0f}, {cycle_high_date}) — em drawdown, +{pct_from_low:.0f}% acima do mínimo."
            )
        else:
            cycle_stats_line = (
                f"Cycle high: ${cycle_high:,.0f} ({cycle_high_date}). Cycle low: ${cycle_low:,.0f} ({cycle_low_date}). Current price is {abs(pct_from_high):.0f}% below the peak."
                if en else
                f"Máximo do ciclo: ${cycle_high:,.0f} ({cycle_high_date}). Mínimo: ${cycle_low:,.0f} ({cycle_low_date}). Preço actual {abs(pct_from_high):.0f}% abaixo do pico."
            )
    else:
        cycle_stats_line = ""

    # ── Posição no ciclo ───────────────────────────────────────────────────────
    if cycle_day < 365:
        cycle_line = (
            f"We are on day {cycle_day} of the post-halving cycle ({cycle_pct}%) — typically an accumulation and early expansion phase."
            if en else
            f"Estamos no dia {cycle_day} do ciclo pós-halving ({cycle_pct}%) — fase tipicamente de acumulação e início de expansão."
        )
    elif cycle_day < 730:
        cycle_line = (
            f"Day {cycle_day} of the cycle ({cycle_pct}%) — historically the bull market acceleration phase occurs in this period."
            if en else
            f"No dia {cycle_day} de ciclo ({cycle_pct}%) — historicamente a fase de aceleração de bull market ocorre neste período."
        )
    elif cycle_day < 1100:
        cycle_line = (
            f"Day {cycle_day} of the cycle ({cycle_pct}%) — previous cycles saw topping and early distribution in this phase."
            if en else
            f"No dia {cycle_day} de ciclo ({cycle_pct}%) — fase de topo e início de distribuição em ciclos anteriores."
        )
    else:
        cycle_line = (
            f"Day {cycle_day} of the cycle ({cycle_pct}%) — late cycle phase, approaching the next halving in 2028."
            if en else
            f"No dia {cycle_day} de ciclo ({cycle_pct}%) — fase final do ciclo, próximo do próximo halving de 2028."
        )

    # ── Conclusão ──────────────────────────────────────────────────────────────
    score = 0
    if ch24 and ch24 > 1: score += 1
    if ch24 and ch24 < -1: score -= 1
    if fg_val > 55: score += 1
    if fg_val < 45: score -= 1
    if bullish_n > bearish_n: score += 1
    if bearish_n > bullish_n: score -= 1
    if cycle_high and btc and btc >= cycle_high * 0.9: score += 1
    if cycle_low and btc and btc <= cycle_low * 1.1: score -= 1

    if score >= 2:
        conclusion = (
            "Overall context is bullish: price, sentiment and technical indicators are aligning positively."
            if en else
            "O contexto geral é bullish: preço, sentimento e indicadores técnicos alinham-se positivamente."
        )
    elif score <= -2:
        conclusion = (
            "Overall context is bearish: price pressure, negative sentiment and technical indicators are converging negatively."
            if en else
            "O contexto geral é bearish: pressão de preço, sentimento negativo e indicadores técnicos convergem negativamente."
        )
    else:
        conclusion = (
            "Context is sideways/uncertain: indicators are not pointing in a clear direction — caution and wait for trend confirmation."
            if en else
            "O contexto é sideways/incerto: os indicadores não apontam numa direcção clara — cautela e aguardar confirmação de tendência."
        )

    parts = [price_line, news_line, fg_line]
    if dom_line: parts.append(dom_line)
    if cycle_stats_line: parts.append(cycle_stats_line)
    parts.append(cycle_line)
    parts.append(conclusion)
    return " ".join(parts)


@app.route("/api/agent-summary")
def api_agent_summary():
    lang = request.args.get("lang", "pt")[:2].lower()
    cache_key = f"agent_summary_{lang}"

    cached = cache_get(cache_key)
    if cached:
        return jsonify(cached), 200

    # Garantir que os preços estão disponíveis antes de gerar o resumo
    if not cache_get("prices"):
        cache_set("prices", fetch_prices())
    if not cache_get("feargreed"):
        cache_set("feargreed", fetch_feargreed())

    if not GROQ_API_KEY:
        summary = build_smart_summary(lang)
        result = {"summary": summary, "generated_at": datetime.now().strftime("%H:%M")}
        cache_set(cache_key, result)
        return jsonify(result), 200

    try:
        news_data = cache_get("news") or {}
        headlines = news_data.get("items", [])[:6]
        news_block = (
            "\n".join(
                f"- [{n['source']}] {n['title']} [{n['sentiment'].upper()}]"
                for n in headlines
            )
            if headlines
            else ("No recent news available." if lang == "en" else "Sem notícias disponíveis de momento.")
        )

        if lang == "en":
            user_msg = (
                f"Recent news (last few hours):\n{news_block}\n\n"
                "Based on the real-time market data AND the news above, write a professional and conclusive market analysis. "
                "Required structure (4 to 6 flowing sentences, no bullet points):\n"
                "1. Current price state and momentum (include real price values and changes).\n"
                "2. What the news signals — dominant theme and expected impact.\n"
                "3. Reading of Fear & Greed, dominance and position in the post-halving cycle.\n"
                "4. Clear and specific conclusion: bullish, bearish or sideways context, and why.\n"
                "Be analytical, specific with numbers, no markdown, no emojis. English. Professional tone."
            )
        else:
            user_msg = (
                f"Notícias recentes das fontes (últimas horas):\n{news_block}\n\n"
                "Com base nos dados de mercado em tempo real E nas notícias acima, "
                "escreve uma análise de mercado profissional e conclusiva. "
                "Estrutura obrigatória (4 a 6 frases corridas, sem bullet points):\n"
                "1. Estado actual do preço e momentum (inclui valores reais de preço e variações).\n"
                "2. O que as notícias sinalizam — tema dominante e impacto esperado.\n"
                "3. Leitura do Fear & Greed, dominância e posicionamento no ciclo pós-halving.\n"
                "4. Conclusão clara e específica: contexto bullish, bearish ou sideways, e porquê.\n"
                "Sê analítico, específico com números, sem markdown, sem emojis. "
                "Português de Portugal. Tom profissional e esclarecedor."
            )

        summary = call_groq(
            build_system(lang=lang),
            [{"role": "user", "content": user_msg}],
            max_tokens=600,
        )
        result = {"summary": summary, "generated_at": datetime.now().strftime("%H:%M")}
        cache_set(cache_key, result)
        return jsonify(result), 200
    except RuntimeError as e:
        return jsonify({"summary": str(e), "generated_at": "--"}), 503
    except Exception as e:
        logger.error(f"Agent summary: {e}")
        return jsonify({"summary": "Summary unavailable." if lang == "en" else "Resumo indisponível.", "generated_at": "--"}), 500


@app.route("/api/agent-chat", methods=["POST"])
def api_agent_chat():
    if not GROQ_API_KEY:
        return jsonify(
            {"reply": "Configura a variável GROQ_API_KEY no Render → Environment."}
        ), 200
    body = request.get_json(force=True, silent=True) or {}
    messages = body.get("messages", [])
    snapshot = body.get("snapshot", {})
    sid = (
        body.get("session_id")
        or hashlib.md5((request.remote_addr or "anon").encode()).hexdigest()[:12]
    )
    if not messages:
        return jsonify({"reply": "Sem mensagens."}), 400
    last = next((m["content"] for m in reversed(messages) if m["role"] == "user"), None)
    if last:
        db_save(sid, "user", last)
        db_track(last)
    full = db_history(sid, 10) + messages[-3:]
    try:
        reply = call_groq(build_system(snapshot), full, max_tokens=500)
        db_save(sid, "agent", reply)
        return jsonify({"reply": reply, "session_id": sid}), 200
    except RuntimeError as e:
        return jsonify({"reply": str(e)}), 503
    except Exception as e:
        logger.error(f"Agent chat: {e}")
        return jsonify({"reply": "Erro. Tenta novamente."}), 500


@app.route("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "data_available": DATA_FILE.exists(),
        }
    ), 200


@app.route("/api/ping")
def api_ping():
    """Keep-alive para UptimeRobot — refresca dados se tiverem mais de 30 min."""
    data = get_dashboard_data()
    updated = data.get("updated", "")
    needs_refresh = False
    try:
        last = datetime.strptime(updated, "%d/%m/%Y %H:%M")
        needs_refresh = (datetime.now() - last).total_seconds() > 1800
    except Exception:
        needs_refresh = True
    if needs_refresh:
        import subprocess, sys

        try:
            gen = BASE_DIR / "generate_data.py"
            if gen.exists():
                subprocess.Popen(
                    [sys.executable, str(gen)], cwd=str(BASE_DIR), env={**os.environ}
                )
        except Exception:
            pass
    return jsonify(
        {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "data_updated": updated,
            "refresh_triggered": needs_refresh,
        }
    ), 200


@app.route("/api/config")
def api_config():
    return jsonify(
        {
            "groq_key_set": bool(GROQ_API_KEY),
            "groq_model": GROQ_MODEL,
            "data_file_exists": DATA_FILE.exists(),
            "db": db_stats(),
        }
    ), 200


@app.route("/api/stats")
def api_stats():
    return jsonify({**db_stats(), "top_questions": db_top(10)}), 200


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint não encontrado"}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Erro interno do servidor"}), 500


# ── yfinance helpers ─────────────────────────────────────────────────────────
def _yf_fast_price(ticker, lo=0, hi=1e9):
    """Preço actual via yfinance (gere cookies/crumb automaticamente)."""
    try:
        fi = _yf.Ticker(ticker).fast_info
        price = fi.get("lastPrice") or fi.get("previousClose") or fi.get("regularMarketPrice")
        if price and lo < float(price) < hi:
            return round(float(price), 4)
    except Exception:
        pass
    return None


def _yf_history_bars(ticker, period, interval):
    """Devolve lista de (ts_s, open, high, low, close, vol) via yfinance."""
    try:
        df = _yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=True)
        if df is None or df.empty:
            return []
        result = []
        for idx, row in df.iterrows():
            cl = float(row["Close"])
            if cl <= 0:
                continue
            ts = int(idx.timestamp())
            result.append((ts, float(row["Open"]), float(row["High"]),
                           float(row["Low"]), cl, float(row.get("Volume", 0))))
        return result
    except Exception:
        return []


# ── OHLC HISTÓRICO para gráficos ─────────────────────────────────────────────
from collections import defaultdict as _dd

_ohlc_cache = {}
OHLC_TTL = {"weekly": 3600, "monthly": 7200, "yearly": 86400}


def _binance_klines(interval, limit, start_ms=None):
    """Binance klines: devolve lista de (ts_s, open, high, low, close, vol)."""
    url = (f"https://api.binance.com/api/v3/klines"
           f"?symbol=BTCUSDT&interval={interval}&limit={limit}")
    if start_ms:
        url += f"&startTime={start_ms}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    r.raise_for_status()
    result = []
    for k in r.json():
        cl = float(k[4])
        if cl > 0:
            result.append((int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), cl, float(k[5])))
    return result

# ── Yahoo Finance crumb (obrigatório desde 2025) ──────────────────────────────
_yf_crumb_cache: dict = {"crumb": None, "ts": 0.0}
_yf_session = requests.Session()
_yf_crumb_lock = threading.Lock()


def _get_yf_crumb():
    """Devolve crumb válido para Yahoo Finance v8 API (cache 10 min)."""
    with _yf_crumb_lock:
        if _yf_crumb_cache["crumb"] and (time.time() - _yf_crumb_cache["ts"]) < 600:
            return _yf_crumb_cache["crumb"]
        hdrs = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        _yf_session.headers.update(hdrs)
        for base in ("https://finance.yahoo.com/", "https://www.yahoo.com/"):
            try:
                _yf_session.get(base, timeout=10)
                break
            except Exception:
                continue
        for endpoint in (
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
        ):
            try:
                r = _yf_session.get(endpoint, timeout=8)
                if r.status_code == 200:
                    crumb = r.text.strip()
                    if crumb and len(crumb) > 2 and crumb.lower() != "null":
                        _yf_crumb_cache["crumb"] = crumb
                        _yf_crumb_cache["ts"] = time.time()
                        logger.info(f"Yahoo Finance crumb OK ({endpoint.split('/')[2]})")
                        return crumb
            except Exception as e:
                logger.warning(f"Yahoo crumb {endpoint}: {e}")
        return None


def _stooq_daily_btc(ticker, lo, hi, btc_usd, days=6):
    """Últimos N dias do Stooq convertidos a rácio BTC — fallback para sparklines."""
    try:
        d2 = datetime.utcnow()
        d1 = d2 - timedelta(days=days + 7)
        url = (f"https://stooq.com/q/d/l/?s={ticker}"
               f"&d1={d1.strftime('%Y%m%d')}&d2={d2.strftime('%Y%m%d')}&i=d")
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        r.raise_for_status()
        lines = [l.strip() for l in r.text.strip().split("\n") if l.strip()][1:]
        bars = []
        for line in lines[-days:]:
            parts = line.split(",")
            if len(parts) < 5:
                continue
            try:
                close = float(parts[4])
                ts = int(datetime.strptime(parts[0], "%Y-%m-%d").timestamp())
                if lo < close < hi and btc_usd > 0:
                    bars.append({"t": ts, "v": round(close / btc_usd, 8)})
            except Exception:
                continue
        return bars
    except Exception as e:
        logger.warning(f"Stooq daily {ticker}: {e}")
        return []


@app.route("/api/ohlc/<timeframe>")
def api_ohlc(timeframe):
    """
    OHLC histórico BTC com fontes por prioridade:
      weekly  → Kraken primary, Yahoo Finance fallback, Bybit last resort
      monthly → Yahoo Finance primary (1mo), Bybit fallback
      yearly  → Yahoo Finance monthly → group by year, Bybit fallback
    """
    if timeframe not in ("weekly", "monthly", "yearly"):
        return jsonify({"error": "timeframe inválido"}), 400

    cached = _ohlc_cache.get(timeframe)
    if cached and (time.time() - cached["ts"]) < OHLC_TTL.get(timeframe, 3600):
        return jsonify(cached["data"]), 200

    H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    bars = None
    errors = []

    # ── 1. KRAKEN — semanal ────────────────────────────────────────────────────
    if timeframe == "weekly":
        try:
            r = requests.get(
                "https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=10080",
                headers=H,
                timeout=15,
            )
            r.raise_for_status()
            d = r.json()
            if d.get("error") and d["error"]:
                raise ValueError(d["error"])
            result_key = next((k for k in d.get("result", {}) if k != "last"), None)
            if not result_key:
                raise ValueError("Kraken sem par válido")
            raw = d["result"][result_key]
            bars = []
            for c in raw:
                cl = float(c[4])
                if cl <= 0:
                    continue
                dt = datetime.utcfromtimestamp(int(c[0]))
                bars.append(
                    {
                        "t": int(c[0]),
                        "o": round(float(c[1]), 2),
                        "h": round(float(c[2]), 2),
                        "l": round(float(c[3]), 2),
                        "c": round(cl, 2),
                        "v": round(float(c[6]), 2),
                        "avg": round((float(c[1]) + cl) / 2, 2),
                        "label": dt.strftime("%d %b %y"),
                    }
                )
            if not bars:
                raise ValueError("Kraken retornou 0 barras")
            logger.info(f"OHLC weekly: {len(bars)} barras via Kraken")
        except Exception as e:
            errors.append(f"Kraken: {e}")
            bars = None

    # ── 1b. yfinance — mensal/anual primary (weekly não precisa, Kraken OK) ─────
    if bars is None and timeframe != "weekly":
        try:
            yf_interval = "1mo"
            yf_period   = "max"
            raw_yfi = _yf_history_bars("BTC-USD", yf_period, yf_interval)
            if not raw_yfi:
                raise ValueError("yfinance: 0 barras")

            def lbl_yfi(ts, tf):
                d2 = datetime.utcfromtimestamp(ts)
                return d2.strftime("%b %Y") if tf == "monthly" else str(d2.year)

            if timeframe == "monthly":
                bars = [{"t": b[0], "o": round(b[1], 2), "h": round(b[2], 2),
                         "l": round(b[3], 2), "c": round(b[4], 2), "v": round(b[5], 2),
                         "avg": round((b[1] + b[4]) / 2, 2),
                         "label": lbl_yfi(b[0], "monthly")} for b in raw_yfi]
            else:
                yd0 = _dd(list)
                for b in raw_yfi:
                    yd0[datetime.utcfromtimestamp(b[0]).year].append(b)
                bars = []
                for year in sorted(yd0.keys()):
                    yc = yd0[year]
                    bars.append({"t": yc[0][0], "o": round(yc[0][1], 2),
                                 "h": round(max(b[2] for b in yc), 2),
                                 "l": round(min(b[3] for b in yc), 2),
                                 "c": round(yc[-1][4], 2),
                                 "v": round(sum(b[5] for b in yc), 2),
                                 "avg": round(sum(b[4] for b in yc) / len(yc), 2),
                                 "label": str(year)})
            if not bars:
                raise ValueError("yfinance: 0 barras após agrupamento")
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via yfinance")
        except Exception as e:
            errors.append(f"yfinance: {e}")
            bars = None

    # ── 2. YAHOO FINANCE raw — mensal/anual (fallback), semanal (fallback) ────
    if bars is None:
        try:
            if timeframe == "weekly":
                url_yf = "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?interval=1wk&range=5y"
            else:
                url_yf = "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?interval=1mo&range=10y"
            _yf_c = _get_yf_crumb()
            if _yf_c:
                url_yf += f"&crumb={_yf_c}"
            ry = _yf_session.get(url_yf, timeout=15)
            ry.raise_for_status()
            dy = ry.json()
            res_yf = dy["chart"]["result"][0]
            tss    = res_yf["timestamp"]
            qt     = res_yf["indicators"]["quote"][0]
            opens  = qt.get("open",   [None] * len(tss))
            highs  = qt.get("high",   [None] * len(tss))
            lows   = qt.get("low",    [None] * len(tss))
            closes = qt.get("close",  [None] * len(tss))
            vols   = qt.get("volume", [0]    * len(tss))

            raw = []
            for i, ts in enumerate(tss):
                cl = closes[i]
                if cl is None or cl <= 0:
                    continue
                raw.append((ts, opens[i] or cl, highs[i] or cl, lows[i] or cl, cl, vols[i] or 0))

            if not raw:
                raise ValueError("Yahoo Finance sem velas válidas")

            def lbl_yf(ts, tf):
                d2 = datetime.utcfromtimestamp(ts)
                return d2.strftime("%d %b %y") if tf == "weekly" else d2.strftime("%b %Y")

            if timeframe in ("weekly", "monthly"):
                bars = [{
                    "t": b[0], "o": round(b[1], 2), "h": round(b[2], 2),
                    "l": round(b[3], 2), "c": round(b[4], 2), "v": round(b[5], 2),
                    "avg": round((b[1] + b[4]) / 2, 2),
                    "label": lbl_yf(b[0], timeframe),
                } for b in raw]
            else:
                yd = _dd(list)
                for b in raw:
                    yd[datetime.utcfromtimestamp(b[0]).year].append(b)
                bars = []
                for year in sorted(yd.keys()):
                    yc = yd[year]
                    bars.append({
                        "t": yc[0][0],
                        "o": round(yc[0][1], 2),
                        "h": round(max(b[2] for b in yc), 2),
                        "l": round(min(b[3] for b in yc), 2),
                        "c": round(yc[-1][4], 2),
                        "v": round(sum(b[5] for b in yc), 2),
                        "avg": round(sum(b[4] for b in yc) / len(yc), 2),
                        "label": str(year),
                    })

            logger.info(f"OHLC {timeframe}: {len(bars)} barras via Yahoo Finance")
        except Exception as e:
            errors.append(f"Yahoo Finance: {e}")
            bars = None

    # ── 3. BYBIT — fallback ───────────────────────────────────────────────────
    if bars is None:
        try:
            if timeframe == "weekly":
                url_by = "https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=W&limit=250"
            else:
                url_by = "https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=M&limit=180"

            rb = requests.get(url_by, headers=H, timeout=15)
            rb.raise_for_status()
            db = rb.json()
            if db.get("retCode") != 0:
                raise ValueError(f"Bybit: {db.get('retMsg', 'erro')}")
            klines = list(reversed(db["result"]["list"]))
            if not klines:
                raise ValueError("Bybit sem dados")

            def lbl_by(ts_ms, tf):
                d2 = datetime.utcfromtimestamp(int(ts_ms) / 1000)
                if tf == "weekly":
                    return d2.strftime("%d %b %y")
                elif tf == "monthly":
                    return d2.strftime("%b %Y")
                return str(d2.year)

            if timeframe in ("weekly", "monthly"):
                bars = []
                for k in klines:
                    cl = float(k[4])
                    if cl <= 0:
                        continue
                    bars.append({
                        "t": int(k[0]) // 1000,
                        "o": round(float(k[1]), 2), "h": round(float(k[2]), 2),
                        "l": round(float(k[3]), 2), "c": round(cl, 2),
                        "v": round(float(k[5]), 2),
                        "avg": round((float(k[1]) + cl) / 2, 2),
                        "label": lbl_by(k[0], timeframe),
                    })
            else:
                yd2 = _dd(list)
                for k in klines:
                    yd2[datetime.utcfromtimestamp(int(k[0]) / 1000).year].append(k)
                bars = []
                for year in sorted(yd2.keys()):
                    yc = yd2[year]
                    cl = float(yc[-1][4])
                    if cl <= 0:
                        continue
                    bars.append({
                        "t": int(yc[0][0]) // 1000,
                        "o": round(float(yc[0][1]), 2),
                        "h": round(max(float(k[2]) for k in yc), 2),
                        "l": round(min(float(k[3]) for k in yc), 2),
                        "c": round(cl, 2),
                        "v": round(sum(float(k[5]) for k in yc), 2),
                        "avg": round(sum(float(k[4]) for k in yc) / len(yc), 2),
                        "label": str(year),
                    })

            if not bars:
                raise ValueError("Bybit retornou 0 barras")
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via Bybit")
        except Exception as e:
            errors.append(f"Bybit: {e}")
            bars = None

    # ── 4. BINANCE — último fallback ──────────────────────────────────────────
    if bars is None:
        try:
            bn_interval = {"weekly": "1w", "monthly": "1M", "yearly": "1M"}[timeframe]
            bn_limit    = {"weekly": 250,  "monthly": 180,  "yearly": 180}[timeframe]
            raw_bn = _binance_klines(bn_interval, bn_limit)
            if not raw_bn:
                raise ValueError("Binance: 0 barras")

            def lbl_bn(ts, tf):
                d2 = datetime.utcfromtimestamp(ts)
                if tf == "weekly":   return d2.strftime("%d %b %y")
                if tf == "monthly":  return d2.strftime("%b %Y")
                return str(d2.year)

            if timeframe in ("weekly", "monthly"):
                bars = [{"t": b[0], "o": round(b[1], 2), "h": round(b[2], 2),
                         "l": round(b[3], 2), "c": round(b[4], 2), "v": round(b[5], 2),
                         "avg": round((b[1] + b[4]) / 2, 2),
                         "label": lbl_bn(b[0], timeframe)} for b in raw_bn]
            else:
                yd3 = _dd(list)
                for b in raw_bn:
                    yd3[datetime.utcfromtimestamp(b[0]).year].append(b)
                bars = []
                for year in sorted(yd3.keys()):
                    yc = yd3[year]
                    bars.append({"t": yc[0][0], "o": round(yc[0][1], 2),
                                 "h": round(max(b[2] for b in yc), 2),
                                 "l": round(min(b[3] for b in yc), 2),
                                 "c": round(yc[-1][4], 2),
                                 "v": round(sum(b[5] for b in yc), 2),
                                 "avg": round(sum(b[4] for b in yc) / len(yc), 2),
                                 "label": str(year)})
            if not bars:
                raise ValueError("0 barras após agrupamento")
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via Binance")
        except Exception as e:
            errors.append(f"Binance: {e}")
            bars = None

    if not bars:
        logger.error(f"OHLC {timeframe} todas as fontes falharam: {errors}")
        return jsonify(
            {"error": "Todas as fontes falharam: " + " | ".join(errors)}
        ), 502

    result = {
        "bars": bars,
        "timeframe": timeframe,
        "updated": datetime.now().strftime("%H:%M"),
    }
    _ohlc_cache[timeframe] = {"data": result, "ts": time.time()}
    return jsonify(result), 200


HALVING_4_MS = 1713484800000  # 19 Abril 2024


@app.route("/api/cycle-stats")
def api_cycle_stats():
    """Máximo e mínimo de BTC desde o halving de Abril 2024 (ciclo actual)."""
    cached = _ohlc_cache.get("cycle_stats")
    if cached and (time.time() - cached["ts"]) < 3600:
        return jsonify(cached["data"]), 200

    H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    candles = None
    halving_s = HALVING_4_MS // 1000

    # Primary: Yahoo Finance — velas diárias desde o halving
    try:
        _yf_c = _get_yf_crumb()
        ry = _yf_session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD"
            f"?interval=1d&period1={halving_s}&period2={int(time.time())}"
            + (f"&crumb={_yf_c}" if _yf_c else ""),
            timeout=15,
        )
        ry.raise_for_status()
        dy = ry.json()
        res_yf = dy["chart"]["result"][0]
        tss   = res_yf["timestamp"]
        qt    = res_yf["indicators"]["quote"][0]
        highs = qt.get("high", [None] * len(tss))
        lows  = qt.get("low",  [None] * len(tss))
        candles = [
            (ts, hi, lo)
            for ts, hi, lo in zip(tss, highs, lows)
            if hi is not None and lo is not None and hi > 0
        ]
        if not candles:
            raise ValueError("Yahoo Finance: 0 candles válidos")
        logger.info(f"CycleStats: {len(candles)} dias via Yahoo Finance")
    except Exception as e:
        logger.warning(f"CycleStats Yahoo: {e}")
        candles = None

    # Fallback: Bybit daily
    if candles is None:
        try:
            rb = requests.get(
                f"https://api.bybit.com/v5/market/kline?category=spot"
                f"&symbol=BTCUSDT&interval=D&start={HALVING_4_MS}&limit=1000",
                headers=H,
                timeout=15,
            )
            rb.raise_for_status()
            db = rb.json()
            if db.get("retCode") != 0:
                raise ValueError(f"Bybit: {db.get('retMsg')}")
            klines = list(reversed(db["result"]["list"]))
            candles = [
                (int(k[0]) // 1000, float(k[2]), float(k[3]))
                for k in klines
                if float(k[4]) > 0 and int(k[0]) // 1000 >= halving_s
            ]
            if not candles:
                raise ValueError("Bybit: 0 candles válidos")
            logger.info(f"CycleStats: {len(candles)} dias via Bybit")
        except Exception as e2:
            logger.warning(f"CycleStats Bybit: {e2}")
            candles = None

    # Fallback: Binance daily
    if candles is None:
        try:
            raw_bn = _binance_klines("1d", 1000, start_ms=HALVING_4_MS)
            candles = [(b[0], b[2], b[3]) for b in raw_bn if b[0] >= halving_s]
            if not candles:
                raise ValueError("Binance: 0 candles")
            logger.info(f"CycleStats: {len(candles)} dias via Binance")
        except Exception as e3:
            logger.error(f"CycleStats Binance: {e3}")
            return jsonify({"error": str(e3)}), 502

    if not candles:
        return jsonify({"error": "sem dados"}), 502

    high_val = max(c[1] for c in candles)
    low_val  = min(c[2] for c in candles)
    high_ts  = next(c[0] for c in candles if c[1] == high_val)
    low_ts   = next(c[0] for c in candles if c[2] == low_val)

    def fmt_date(ts):
        return datetime.utcfromtimestamp(ts).strftime("%b '%y")

    result = {
        "cycle_high": round(high_val, 0),
        "cycle_high_date": fmt_date(high_ts),
        "cycle_low": round(low_val, 0),
        "cycle_low_date": fmt_date(low_ts),
    }
    _ohlc_cache["cycle_stats"] = {"data": result, "ts": time.time()}
    logger.info(
        f"CycleStats: high=${high_val:,.0f} ({fmt_date(high_ts)}), "
        f"low=${low_val:,.0f} ({fmt_date(low_ts)})"
    )
    return jsonify(result), 200


@app.route("/api/ohlc/cycle-daily")
def api_ohlc_cycle_daily():
    """Série diária de closes BTC desde o halving de Abril 2024."""
    cached = _ohlc_cache.get("cycle_daily")
    if cached and (time.time() - cached["ts"]) < 3600:
        return jsonify(cached["data"]), 200

    H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    halving_s = HALVING_4_MS // 1000
    bars = None

    try:
        _yf_c = _get_yf_crumb()
        ry = _yf_session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD"
            f"?interval=1d&period1={halving_s}&period2={int(time.time())}"
            + (f"&crumb={_yf_c}" if _yf_c else ""),
            timeout=15,
        )
        ry.raise_for_status()
        dy = ry.json()
        res_yf = dy["chart"]["result"][0]
        tss    = res_yf["timestamp"]
        closes = res_yf["indicators"]["quote"][0].get("close", [])
        raw = [(ts, cl) for ts, cl in zip(tss, closes) if cl and cl > 0]
        if not raw:
            raise ValueError("0 bars")
        bars = [{"t": ts, "c": round(cl, 2), "day": i} for i, (ts, cl) in enumerate(raw)]
        logger.info(f"cycle-daily: {len(bars)} dias via Yahoo Finance")
    except Exception as e:
        logger.warning(f"cycle-daily Yahoo: {e}")

    if not bars:
        try:
            rb = requests.get(
                f"https://api.bybit.com/v5/market/kline?category=spot"
                f"&symbol=BTCUSDT&interval=D&start={HALVING_4_MS}&limit=1000",
                headers=H, timeout=15,
            )
            rb.raise_for_status()
            db = rb.json()
            if db.get("retCode") != 0:
                raise ValueError(db.get("retMsg"))
            klines = [k for k in reversed(db["result"]["list"]) if float(k[4]) > 0 and int(k[0]) // 1000 >= halving_s]
            if not klines:
                raise ValueError("0 bars")
            bars = [{"t": int(k[0]) // 1000, "c": round(float(k[4]), 2), "day": i} for i, k in enumerate(klines)]
            logger.info(f"cycle-daily: {len(bars)} dias via Bybit")
        except Exception as e:
            logger.warning(f"cycle-daily Bybit: {e}")

    if not bars:
        try:
            raw_bn = _binance_klines("1d", 1000, start_ms=HALVING_4_MS)
            bars = [{"t": b[0], "c": round(b[4], 2), "day": i}
                    for i, b in enumerate(raw_bn) if b[0] >= HALVING_4_MS // 1000]
            if not bars:
                raise ValueError("0 bars")
            logger.info(f"cycle-daily: {len(bars)} dias via Binance")
        except Exception as e:
            logger.error(f"cycle-daily Binance: {e}")
            return jsonify({"error": str(e)}), 502

    halving_price = bars[0]["c"] if bars else 63800
    result = {"bars": bars, "halving_price": round(halving_price, 2), "updated": datetime.now().strftime("%H:%M")}
    _ohlc_cache["cycle_daily"] = {"data": result, "ts": time.time()}
    return jsonify(result), 200


@app.route("/api/ohlc/assets24h")
def api_assets24h():
    """Dados horários 24h para sparklines de ETH/BTC, Gold/BTC e Oil/BTC."""
    cached = _ohlc_cache.get("assets24h")
    if cached and (time.time() - cached["ts"]) < 300:
        return jsonify(cached["data"]), 200

    H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    result = {"eth": [], "gold": [], "sp500": [], "oil": [], "silver": [], "nasdaq": [], "updated": datetime.now().strftime("%H:%M")}

    _yf_c = _get_yf_crumb()
    _crumb_qs = f"&crumb={_yf_c}" if _yf_c else ""

    # ── Mapa BTC/USD 1h (partilhado por Gold e Oil) ───────────────────────────
    # Reutiliza cache h24 se disponível para evitar chamada dupla ao Kraken
    btc_map = {}
    h24_c = _ohlc_cache.get("h24")
    if h24_c and h24_c.get("data", {}).get("bars"):
        for b in h24_c["data"]["bars"]:
            btc_map[b["t"]] = b["c"]
    if not btc_map:
        try:
            rb = requests.get(
                "https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60",
                headers=H, timeout=10,
            )
            rb.raise_for_status()
            db_k = rb.json().get("result", {})
            btc_key = next((k for k in db_k if k != "last"), None)
            if btc_key:
                for c in db_k[btc_key][-25:]:
                    btc_map[int(c[0])] = float(c[4])
        except Exception as e:
            logger.warning(f"Assets24h BTC map: {e}")
    btc_ts_sorted = sorted(btc_map.keys())

    def _nearest_btc(ts):
        """Devolve o preço BTC mais próximo do timestamp dado (tolerância 2h)."""
        if not btc_ts_sorted:
            return None
        idx = bisect.bisect_left(btc_ts_sorted, ts)
        candidates = btc_ts_sorted[max(0, idx - 1): idx + 2]
        nearest = min(candidates, key=lambda x: abs(x - ts))
        if abs(nearest - ts) < 7200 and btc_map[nearest] > 0:
            return btc_map[nearest]
        return None

    # ── ETH/BTC: Kraken ETHXBT 1h OHLC (par directo, sem conversão) ──────────
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC?pair=ETHXBT&interval=60",
            headers=H, timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("result", {})
        key = next((k for k in d if k != "last"), None)
        if key:
            candles = d[key][-25:]
            result["eth"] = [{"t": int(c[0]), "v": float(c[4])} for c in candles]
            logger.info(f"Assets24h ETH/BTC: {len(result['eth'])} velas")
    except Exception as e:
        logger.warning(f"Assets24h ETH/BTC: {e}")

    # btc_usd para fallback Stooq (conversão directa sem mapa horário)
    _cached_p = cache_get("prices") or {}
    _btc_spot = _cached_p.get("btc_usd") or (list(btc_map.values())[-1] if btc_map else 0)

    def _yf_hourly(yf_ticker, result_key, lo, hi, decimals,
                   stooq_ticker, stooq_lo, stooq_hi):
        """yfinance 1h → Yahoo raw 1h → Stooq daily como fallbacks."""
        bars = []

        # 1) yfinance hourly (mais fiável que raw requests)
        try:
            raw_yfi = _yf_history_bars(yf_ticker, "2d", "1h")
            if raw_yfi and btc_map:
                for b in raw_yfi[-25:]:
                    cl = b[4]
                    if not (lo < cl < hi):
                        continue
                    bv = _nearest_btc(b[0])
                    if bv:
                        bars.append({"t": b[0], "v": round(cl / bv, decimals)})
                if bars:
                    logger.info(f"Assets24h {result_key}: {len(bars)} pts (yfinance)")
        except Exception as e:
            logger.warning(f"Assets24h {result_key} yfinance: {e}")

        # 2) Yahoo raw
        if len(bars) < 2:
            try:
                ry = _yf_session.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_ticker}"
                    f"?interval=1h&range=2d{_crumb_qs}",
                    timeout=10,
                )
                ry.raise_for_status()
                cr = (ry.json().get("chart") or {}).get("result") or []
                if not cr:
                    raise ValueError(f"Yahoo sem dados {yf_ticker}")
                tss  = cr[0]["timestamp"]
                clss = cr[0]["indicators"]["quote"][0]["close"]
                if btc_map:
                    raw_bars = []
                    for ts, cl in zip(tss, clss):
                        if cl is None or not (lo < cl < hi):
                            continue
                        bv = _nearest_btc(ts)
                        if bv:
                            raw_bars.append({"t": ts, "v": round(cl / bv, decimals)})
                    if raw_bars:
                        bars = raw_bars[-25:]
                        logger.info(f"Assets24h {result_key}: {len(bars)} pts (Yahoo raw)")
            except Exception as e:
                logger.warning(f"Assets24h {result_key} Yahoo raw: {e}")

        # 3) Stooq daily
        if len(bars) < 2 and stooq_ticker and _btc_spot > 0:
            bars = _stooq_daily_btc(stooq_ticker, stooq_lo, stooq_hi, _btc_spot, days=6)
            if bars:
                logger.info(f"Assets24h {result_key}: {len(bars)} pts (Stooq)")

        result[result_key] = bars

    _yf_hourly("GC=F",   "gold",   1500, 5000, 6, "gc.f",  1500, 5000)
    _yf_hourly("CL=F",   "oil",    30,   200,  8, "cl.f",  30,   200)
    _yf_hourly("%5EGSPC","sp500",  3000, 7000, 6, "^spx",  3000, 7000)
    _yf_hourly("SI=F",   "silver", 5,    200,  8, "si.f",  5,    200)
    _yf_hourly("%5EIXIC","nasdaq", 8000, 30000,6, "^ndq",  8000, 30000)

    _ohlc_cache["assets24h"] = {"data": result, "ts": time.time()}
    return jsonify(result), 200


@app.route("/api/ohlc/h24")
def api_ohlc_h24():
    """Velas horárias das últimas 24h — para sparkline do hero."""
    cached = _ohlc_cache.get("h24")
    if cached and (time.time() - cached["ts"]) < 300:  # cache 5 min
        return jsonify(cached["data"]), 200

    H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60",
            headers=H,
            timeout=10,
        )
        r.raise_for_status()
        d = r.json().get("result", {})
        key = next((k for k in d if k != "last"), None)
        if not key:
            raise ValueError("sem dados Kraken OHLC 1h")
        candles = d[key][-25:]  # últimas 25h (última pode ser incompleta)
        bars = [
            {
                "t": int(c[0]),
                "o": float(c[1]),
                "h": float(c[2]),
                "l": float(c[3]),
                "c": float(c[4]),
            }
            for c in candles
        ]
        result = {"bars": bars, "updated": datetime.now().strftime("%H:%M")}
        _ohlc_cache["h24"] = {"data": result, "ts": time.time()}
        logger.info(f"OHLC h24: {len(bars)} velas horárias")
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"OHLC h24: {e}")
        return jsonify({"error": str(e)}), 502


# ── ARRANQUE ──────────────────────────────────────────────────────────────────
def warm_cache():
    logger.info("A pré-carregar cache...")
    cache_set("prices", fetch_prices())
    cache_set("news", fetch_news())
    cache_set("feargreed", fetch_feargreed())
    logger.info("Cache pronta.")


db_init()
threading.Thread(target=warm_cache, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Bitcoin Intelligence · porta {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
