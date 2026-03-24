import os
import re
import json
import time
import sqlite3
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta

import requests
import feedparser
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

    # ── Petróleo WTI: Yahoo Finance (primário, funciona em cloud) ────────────
    crude_usd = None
    try:
        ry = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/CL=F"
            "?interval=1d&range=5d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        ry.raise_for_status()
        dy = ry.json()
        closes = dy["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            crude_usd = round(closes[-1], 2)
            logger.info(f"WTI (Yahoo): ${crude_usd:.1f}")
    except Exception as e:
        logger.warning(f"Yahoo WTI: {e}")

    # Fallback WTI: Stooq
    if not crude_usd:
        try:
            crude_usd = stooq_val("cl.f", 40, 200)
            if crude_usd:
                logger.info(f"WTI (Stooq cl.f): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"Stooq WTI: {e}")

    # Fallback WTI: EIA DEMO_KEY
    if not crude_usd:
        try:
            r = requests.get(
                "https://api.eia.gov/v2/petroleum/pri/spt/data/"
                "?api_key=DEMO_KEY&frequency=daily&data[0]=value"
                "&facets[series][]=RWTC&sort[0][column]=period"
                "&sort[0][direction]=desc&length=1",
                headers=H,
                timeout=8,
            )
            r.raise_for_status()
            rows = r.json().get("response", {}).get("data", [])
            if rows:
                crude_usd = float(rows[0]["value"])
                logger.info(f"WTI (EIA): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"EIA WTI: {e}")

    # Último recurso: cache anterior
    if not crude_usd:
        cached_prices = cache_get("prices")
        if cached_prices and cached_prices.get("crude_usd"):
            crude_usd = cached_prices["crude_usd"]
            logger.info(f"WTI (cache): ${crude_usd:.1f}")

    if crude_usd and result["btc_usd"]:
        result["crude_usd"] = crude_usd
        result["crude_btc"] = round(crude_usd / result["btc_usd"], 6)

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


# ── OHLC HISTÓRICO para gráficos ─────────────────────────────────────────────
from collections import defaultdict as _dd

_ohlc_cache = {}
OHLC_TTL = {"weekly": 3600, "monthly": 7200, "yearly": 86400}


@app.route("/api/ohlc/<timeframe>")
def api_ohlc(timeframe):
    """
    OHLC histórico BTC com fontes por prioridade:
      weekly  → Kraken primary (interval=10080 min), Binance fallback, CryptoCompare last resort
      monthly → Binance primary (1M interval), CryptoCompare fallback
      yearly  → Binance primary (1M → group by year), CryptoCompare fallback
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

    # ── 2. BINANCE — mensal/anual (primary), semanal (fallback) ──────────────
    if bars is None:
        try:
            BASE_BN = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT"
            if timeframe == "weekly":
                url_bn = f"{BASE_BN}&interval=1w&limit=250"
            elif timeframe == "monthly":
                url_bn = f"{BASE_BN}&interval=1M&limit=96"
            else:
                url_bn = f"{BASE_BN}&interval=1M&limit=180"

            r = requests.get(url_bn, headers=H, timeout=15)
            r.raise_for_status()
            data = r.json()
            if not data or not isinstance(data, list):
                raise ValueError("resposta Binance inválida")

            def lbl_bn(ts_ms, tf):
                dt = datetime.utcfromtimestamp(ts_ms / 1000)
                if tf == "weekly":
                    return dt.strftime("%d %b %y")
                elif tf == "monthly":
                    return dt.strftime("%b %Y")
                else:
                    return str(dt.year)

            if timeframe in ("weekly", "monthly"):
                bars = []
                for c in data:
                    cl = float(c[4])
                    if cl <= 0:
                        continue
                    bars.append(
                        {
                            "t": int(c[0]) // 1000,
                            "o": round(float(c[1]), 2),
                            "h": round(float(c[2]), 2),
                            "l": round(float(c[3]), 2),
                            "c": round(cl, 2),
                            "v": round(float(c[5]), 2),
                            "avg": round((float(c[1]) + cl) / 2, 2),
                            "label": lbl_bn(int(c[0]), timeframe),
                        }
                    )
            else:
                # yearly: aggregate monthly candles by year
                yearly_d = _dd(list)
                for c in data:
                    dt = datetime.utcfromtimestamp(int(c[0]) / 1000)
                    yearly_d[dt.year].append(c)
                bars = []
                for year in sorted(yearly_d.keys()):
                    yc = yearly_d[year]
                    if not yc:
                        continue
                    cl = float(yc[-1][4])
                    if cl <= 0:
                        continue
                    bars.append(
                        {
                            "t": int(yc[0][0]) // 1000,
                            "o": round(float(yc[0][1]), 2),
                            "h": round(max(float(c[2]) for c in yc), 2),
                            "l": round(min(float(c[3]) for c in yc), 2),
                            "c": round(cl, 2),
                            "v": round(sum(float(c[5]) for c in yc), 2),
                            "avg": round(sum(float(c[4]) for c in yc) / len(yc), 2),
                            "label": str(year),
                        }
                    )

            if not bars:
                raise ValueError("Binance retornou 0 barras")
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via Binance")
        except Exception as e:
            errors.append(f"Binance: {e}")
            bars = None

    # ── 3. CRYPTOCOMPARE — fallback ────────────────────────────────────────────
    if bars is None:
        try:
            BASE_CC = (
                "https://min-api.cryptocompare.com/data/v2/histoday?fsym=BTC&tsym=USD"
            )
            if timeframe == "weekly":
                url_cc = f"{BASE_CC}&aggregate=7&limit=250"
            elif timeframe == "monthly":
                url_cc = f"{BASE_CC}&aggregate=30&limit=84"
            else:
                url_cc = f"{BASE_CC}&aggregate=365&limit=14"

            rc = requests.get(url_cc, headers=H, timeout=15)
            rc.raise_for_status()
            dc = rc.json()
            if dc.get("Response") == "Error":
                raise ValueError(dc.get("Message", "CryptoCompare erro"))

            candles = [c for c in dc["Data"]["Data"] if c.get("close", 0) > 0]
            if not candles:
                raise ValueError("CryptoCompare sem velas válidas")

            def lbl_cc(ts, tf):
                dt = datetime.utcfromtimestamp(ts)
                if tf == "weekly":
                    return dt.strftime("%d %b %y")
                elif tf == "monthly":
                    return dt.strftime("%b %Y")
                else:
                    return str(dt.year)

            bars = [
                {
                    "t": int(c["time"]),
                    "o": round(float(c["open"]), 2),
                    "h": round(float(c["high"]), 2),
                    "l": round(float(c["low"]), 2),
                    "c": round(float(c["close"]), 2),
                    "v": round(float(c.get("volumefrom", 0)), 2),
                    "avg": round((float(c["open"]) + float(c["close"])) / 2, 2),
                    "label": lbl_cc(int(c["time"]), timeframe),
                }
                for c in candles
            ]
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via CryptoCompare")
        except Exception as e:
            errors.append(f"CryptoCompare: {e}")
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

    # Primary: Binance — velas diárias desde o halving
    try:
        r = requests.get(
            f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d"
            f"&startTime={HALVING_4_MS}&limit=1000",
            headers=H,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, list):
            raise ValueError("resposta inválida")
        candles = [(int(c[0]) // 1000, float(c[2]), float(c[3])) for c in data]
        logger.info(f"CycleStats: {len(candles)} dias via Binance")
    except Exception as e:
        logger.warning(f"CycleStats Binance: {e}")

    # Fallback: CryptoCompare
    if candles is None:
        try:
            rc = requests.get(
                "https://min-api.cryptocompare.com/data/v2/histoday"
                "?fsym=BTC&tsym=USD&limit=800",
                headers=H,
                timeout=15,
            )
            rc.raise_for_status()
            dc = rc.json()
            if dc.get("Response") == "Error":
                raise ValueError(dc.get("Message", "CryptoCompare erro"))
            all_c = [c for c in dc["Data"]["Data"] if c.get("high", 0) > 0]
            halving_s = HALVING_4_MS // 1000
            candles = [
                (int(c["time"]), float(c["high"]), float(c["low"]))
                for c in all_c if int(c["time"]) >= halving_s
            ]
            logger.info(f"CycleStats: {len(candles)} dias via CryptoCompare")
        except Exception as e2:
            logger.error(f"CycleStats CryptoCompare: {e2}")
            return jsonify({"error": str(e2)}), 502

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
import os
import re
import json
import time
import sqlite3
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta

import requests
import feedparser
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

    # ── Petróleo WTI: Yahoo Finance (primário, funciona em cloud) ────────────
    crude_usd = None
    try:
        ry = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/CL=F"
            "?interval=1d&range=5d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        ry.raise_for_status()
        dy = ry.json()
        closes = dy["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            crude_usd = round(closes[-1], 2)
            logger.info(f"WTI (Yahoo): ${crude_usd:.1f}")
    except Exception as e:
        logger.warning(f"Yahoo WTI: {e}")

    # Fallback WTI: Stooq
    if not crude_usd:
        try:
            crude_usd = stooq_val("cl.f", 40, 200)
            if crude_usd:
                logger.info(f"WTI (Stooq cl.f): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"Stooq WTI: {e}")

    # Fallback WTI: EIA DEMO_KEY
    if not crude_usd:
        try:
            r = requests.get(
                "https://api.eia.gov/v2/petroleum/pri/spt/data/"
                "?api_key=DEMO_KEY&frequency=daily&data[0]=value"
                "&facets[series][]=RWTC&sort[0][column]=period"
                "&sort[0][direction]=desc&length=1",
                headers=H,
                timeout=8,
            )
            r.raise_for_status()
            rows = r.json().get("response", {}).get("data", [])
            if rows:
                crude_usd = float(rows[0]["value"])
                logger.info(f"WTI (EIA): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"EIA WTI: {e}")

    # Último recurso: cache anterior
    if not crude_usd:
        cached_prices = cache_get("prices")
        if cached_prices and cached_prices.get("crude_usd"):
            crude_usd = cached_prices["crude_usd"]
            logger.info(f"WTI (cache): ${crude_usd:.1f}")

    if crude_usd and result["btc_usd"]:
        result["crude_usd"] = crude_usd
        result["crude_btc"] = round(crude_usd / result["btc_usd"], 6)

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


# ── OHLC HISTÓRICO para gráficos ─────────────────────────────────────────────
from collections import defaultdict as _dd

_ohlc_cache = {}
OHLC_TTL = {"weekly": 3600, "monthly": 7200, "yearly": 86400}


@app.route("/api/ohlc/<timeframe>")
def api_ohlc(timeframe):
    """
    OHLC histórico BTC com fontes por prioridade:
      weekly  → Kraken primary (interval=10080 min), Binance fallback, CryptoCompare last resort
      monthly → Binance primary (1M interval), CryptoCompare fallback
      yearly  → Binance primary (1M → group by year), CryptoCompare fallback
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

    # ── 2. BINANCE — mensal/anual (primary), semanal (fallback) ──────────────
    if bars is None:
        try:
            BASE_BN = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT"
            if timeframe == "weekly":
                url_bn = f"{BASE_BN}&interval=1w&limit=250"
            elif timeframe == "monthly":
                url_bn = f"{BASE_BN}&interval=1M&limit=96"
            else:
                url_bn = f"{BASE_BN}&interval=1M&limit=180"

            r = requests.get(url_bn, headers=H, timeout=15)
            r.raise_for_status()
            data = r.json()
            if not data or not isinstance(data, list):
                raise ValueError("resposta Binance inválida")

            def lbl_bn(ts_ms, tf):
                dt = datetime.utcfromtimestamp(ts_ms / 1000)
                if tf == "weekly":
                    return dt.strftime("%d %b %y")
                elif tf == "monthly":
                    return dt.strftime("%b %Y")
                else:
                    return str(dt.year)

            if timeframe in ("weekly", "monthly"):
                bars = []
                for c in data:
                    cl = float(c[4])
                    if cl <= 0:
                        continue
                    bars.append(
                        {
                            "t": int(c[0]) // 1000,
                            "o": round(float(c[1]), 2),
                            "h": round(float(c[2]), 2),
                            "l": round(float(c[3]), 2),
                            "c": round(cl, 2),
                            "v": round(float(c[5]), 2),
                            "avg": round((float(c[1]) + cl) / 2, 2),
                            "label": lbl_bn(int(c[0]), timeframe),
                        }
                    )
            else:
                # yearly: aggregate monthly candles by year
                yearly_d = _dd(list)
                for c in data:
                    dt = datetime.utcfromtimestamp(int(c[0]) / 1000)
                    yearly_d[dt.year].append(c)
                bars = []
                for year in sorted(yearly_d.keys()):
                    yc = yearly_d[year]
                    if not yc:
                        continue
                    cl = float(yc[-1][4])
                    if cl <= 0:
                        continue
                    bars.append(
                        {
                            "t": int(yc[0][0]) // 1000,
                            "o": round(float(yc[0][1]), 2),
                            "h": round(max(float(c[2]) for c in yc), 2),
                            "l": round(min(float(c[3]) for c in yc), 2),
                            "c": round(cl, 2),
                            "v": round(sum(float(c[5]) for c in yc), 2),
                            "avg": round(sum(float(c[4]) for c in yc) / len(yc), 2),
                            "label": str(year),
                        }
                    )

            if not bars:
                raise ValueError("Binance retornou 0 barras")
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via Binance")
        except Exception as e:
            errors.append(f"Binance: {e}")
            bars = None

    # ── 3. CRYPTOCOMPARE — fallback ────────────────────────────────────────────
    if bars is None:
        try:
            BASE_CC = (
                "https://min-api.cryptocompare.com/data/v2/histoday?fsym=BTC&tsym=USD"
            )
            if timeframe == "weekly":
                url_cc = f"{BASE_CC}&aggregate=7&limit=250"
            elif timeframe == "monthly":
                url_cc = f"{BASE_CC}&aggregate=30&limit=84"
            else:
                url_cc = f"{BASE_CC}&aggregate=365&limit=14"

            rc = requests.get(url_cc, headers=H, timeout=15)
            rc.raise_for_status()
            dc = rc.json()
            if dc.get("Response") == "Error":
                raise ValueError(dc.get("Message", "CryptoCompare erro"))

            candles = [c for c in dc["Data"]["Data"] if c.get("close", 0) > 0]
            if not candles:
                raise ValueError("CryptoCompare sem velas válidas")

            def lbl_cc(ts, tf):
                dt = datetime.utcfromtimestamp(ts)
                if tf == "weekly":
                    return dt.strftime("%d %b %y")
                elif tf == "monthly":
                    return dt.strftime("%b %Y")
                else:
                    return str(dt.year)

            bars = [
                {
                    "t": int(c["time"]),
                    "o": round(float(c["open"]), 2),
                    "h": round(float(c["high"]), 2),
                    "l": round(float(c["low"]), 2),
                    "c": round(float(c["close"]), 2),
                    "v": round(float(c.get("volumefrom", 0)), 2),
                    "avg": round((float(c["open"]) + float(c["close"])) / 2, 2),
                    "label": lbl_cc(int(c["time"]), timeframe),
                }
                for c in candles
            ]
            logger.info(f"OHLC {timeframe}: {len(bars)} barras via CryptoCompare")
        except Exception as e:
            errors.append(f"CryptoCompare: {e}")
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

    # Primary: Binance — velas diárias desde o halving
    try:
        r = requests.get(
            f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d"
            f"&startTime={HALVING_4_MS}&limit=1000",
            headers=H,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, list):
            raise ValueError("resposta inválida")
        candles = [(int(c[0]) // 1000, float(c[2]), float(c[3])) for c in data]
        logger.info(f"CycleStats: {len(candles)} dias via Binance")
    except Exception as e:
        logger.warning(f"CycleStats Binance: {e}")

    # Fallback: CryptoCompare
    if candles is None:
        try:
            rc = requests.get(
                "https://min-api.cryptocompare.com/data/v2/histoday"
                "?fsym=BTC&tsym=USD&limit=800",
                headers=H,
                timeout=15,
            )
            rc.raise_for_status()
            dc = rc.json()
            if dc.get("Response") == "Error":
                raise ValueError(dc.get("Message", "CryptoCompare erro"))
            all_c = [c for c in dc["Data"]["Data"] if c.get("high", 0) > 0]
            halving_s = HALVING_4_MS // 1000
            candles = [
                (int(c["time"]), float(c["high"]), float(c["low"]))
                for c in all_c if int(c["time"]) >= halving_s
            ]
            logger.info(f"CycleStats: {len(candles)} dias via CryptoCompare")
        except Exception as e2:
            logger.error(f"CycleStats CryptoCompare: {e2}")
            return jsonify({"error": str(e2)}), 502

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
