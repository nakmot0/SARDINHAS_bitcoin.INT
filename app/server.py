import os
import re
import json
import time
import sqlite3
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime

import requests
import feedparser
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR    = Path(__file__).parent
DATA_FILE   = BASE_DIR / "data.json"
HTML_FILE   = BASE_DIR / "INDEX.HTML"
SOURCES_FILE = BASE_DIR / "sources.txt"

# BD persistente no disco do Render (/data) ou local
_disk = os.environ.get('RENDER_DISK_PATH', str(BASE_DIR))
DB_PATH = Path(_disk) / 'memory.db'

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.1-8b-instant"   # rápido + poupa tokens free tier
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path='')


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
    con.commit(); con.close()
    logger.info(f"DB: {DB_PATH}")

def db_save(sid, role, content):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO conversations (session_id,role,content,created_at) VALUES (?,?,?,?)",
            (sid, role, content, datetime.now().isoformat()))
        con.commit(); con.close()
    except Exception as e:
        logger.warning(f"db_save: {e}")

def db_history(sid, limit=10):
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            "SELECT role,content FROM conversations WHERE session_id=? ORDER BY id DESC LIMIT ?",
            (sid, limit)).fetchall()
        con.close()
        return [{'role': r, 'content': c} for r, c in reversed(rows)]
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
                (qn, datetime.now().isoformat()))
        except Exception:
            con.execute("UPDATE common_questions SET count=count+1,last_seen=? WHERE question=?",
                        (datetime.now().isoformat(), qn))
        con.commit(); con.close()
    except Exception as e:
        logger.warning(f"db_track: {e}")

def db_top(n=5):
    try:
        con = sqlite3.connect(DB_PATH)
        rows = con.execute(
            "SELECT question,count FROM common_questions ORDER BY count DESC LIMIT ?",
            (n,)).fetchall()
        con.close(); return rows
    except Exception:
        return []

def db_stats():
    try:
        con = sqlite3.connect(DB_PATH)
        msgs = con.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
        sess = con.execute("SELECT COUNT(DISTINCT session_id) FROM conversations").fetchone()[0]
        qs   = con.execute("SELECT COUNT(*) FROM common_questions").fetchone()[0]
        con.close()
        return {'messages': msgs, 'sessions': sess, 'unique_questions': qs}
    except Exception:
        return {'messages': 0, 'sessions': 0, 'unique_questions': 0}


# ── CACHE em memória ──────────────────────────────────────────────────────────
_cache = {}
CACHE_TTL = {
    'prices':        60,
    'news':          600,   # notícias: 10 min (evita sobrecarga RSS)
    'feargreed':     3600,
    'agent_summary': 300,
}

def cache_get(key):
    e = _cache.get(key)
    if e and (time.time() - e['ts']) < CACHE_TTL.get(key, 60):
        return e['data']
    return None

def cache_set(key, data):
    _cache[key] = {'data': data, 'ts': time.time()}


# ── HELPER data.json ──────────────────────────────────────────────────────────
def get_dashboard_data():
    try:
        return json.loads(DATA_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {
            "bitcoin": {"price": "Carregando...", "change": "n/d", "dominance": "n/d"},
            "eth":  {"btc": "n/d"},
            "gold": {"btc": "n/d"},
            "dxy":  {"state": "n/d"},
            "editorial": "Aguardando primeira análise...",
            "updated": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }


# ── FETCH PREÇOS (Kraken + Coinpaprika + Stooq) ───────────────────────────────
def stooq_val(ticker, lo, hi):
    """Busca valor numérico do Stooq — funciona em cloud sem bloqueio."""
    r = requests.get(
        f'https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv',
        headers={'User-Agent': 'Mozilla/5.0'}, timeout=12)
    r.raise_for_status()
    for line in reversed([l.strip() for l in r.text.strip().split('\n') if l.strip()][1:]):
        for col in reversed(line.split(',')):
            try:
                v = float(col)
                if lo < v < hi: return v
            except Exception: continue
    return None

def fetch_prices():
    result = {
        'btc_usd': None, 'btc_change_24h': None, 'btc_change_1h': None,
        'eth_btc': None, 'gold_btc': None, 'dominance': None,
        'crude_usd': None, 'crude_btc': None, 'source': [],
    }
    H = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

    # ── Kraken: BTC spot + ETH/BTC ───────────────────────────────────────────
    try:
        r = requests.get('https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHXBT',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        for k in ['XXBTZUSD', 'XBTUSD']:
            if k in d: result['btc_usd'] = float(d[k]['c'][0]); break
        for k in ['XETHXXBT', 'ETHXBT']:
            if k in d: result['eth_btc'] = float(d[k]['c'][0]); break
        result['source'].append('Kraken')
        logger.info(f"BTC: ${result['btc_usd']:,.0f} | ETH/BTC: {result['eth_btc']}")
    except Exception as e:
        logger.warning(f"Kraken ticker: {e}")

    # Fallback BTC: Coinbase
    if result['btc_usd'] is None:
        try:
            r = requests.get('https://api.coinbase.com/v2/prices/BTC-USD/spot',
                             headers=H, timeout=8)
            r.raise_for_status()
            result['btc_usd'] = float(r.json()['data']['amount'])
            result['source'].append('Coinbase')
        except Exception as e:
            logger.warning(f"Coinbase: {e}")

    # ── Kraken OHLC: variação 24h ─────────────────────────────────────────────
    try:
        r = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next((k for k in d if k != 'last'), None)
        if key and len(d[key]) >= 2:
            o, c = float(d[key][-2][1]), float(d[key][-1][4])
            if o > 0: result['btc_change_24h'] = round((c - o) / o * 100, 3)
    except Exception as e:
        logger.warning(f"Kraken 24h: {e}")

    # ── Kraken OHLC: variação 1h ──────────────────────────────────────────────
    try:
        r = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next((k for k in d if k != 'last'), None)
        if key and len(d[key]) >= 2:
            o, c = float(d[key][-2][1]), float(d[key][-1][4])
            if o > 0: result['btc_change_1h'] = round((c - o) / o * 100, 3)
    except Exception as e:
        logger.warning(f"Kraken 1h: {e}")

    # ── Dominância: Coinpaprika (sem rate-limit em cloud) ────────────────────
    try:
        r = requests.get('https://api.coinpaprika.com/v1/global', headers=H, timeout=10)
        r.raise_for_status()
        result['dominance'] = r.json().get('bitcoin_dominance_percentage')
    except Exception as e:
        logger.warning(f"Coinpaprika: {e}")

    # ── Ouro: CoinGecko tether-gold (XAUT) ───────────────────────────────────
    # XAUT é 1 troy oz de ouro tokenizado — preço = preço real do ouro
    gold_usd = None
    try:
        r = requests.get(
            'https://api.coingecko.com/api/v3/simple/price'
            '?ids=tether-gold&vs_currencies=usd',
            headers=H, timeout=12)
        r.raise_for_status()
        gold_usd = r.json().get('tether-gold', {}).get('usd')
        if gold_usd:
            logger.info(f"Ouro (CoinGecko XAUT): ${gold_usd:.0f}")
    except Exception as e:
        logger.warning(f"CoinGecko XAUT: {e}")

    # Fallback ouro: Coinpaprika XAU-gold
    if not gold_usd:
        try:
            r = requests.get('https://api.coinpaprika.com/v1/tickers/xaut-tether-gold',
                             headers=H, timeout=10)
            r.raise_for_status()
            gold_usd = r.json().get('quotes', {}).get('USD', {}).get('price')
            if gold_usd:
                logger.info(f"Ouro (Coinpaprika XAUT): ${gold_usd:.0f}")
        except Exception as e:
            logger.warning(f"Coinpaprika XAUT: {e}")

    if gold_usd and result['btc_usd']:
        result['gold_btc'] = round(float(gold_usd) / result['btc_usd'], 6)

    # ── Petróleo WTI: EIA (US Energy Information Administration) ─────────────
    # API governamental gratuita, sem API key necessária com DEMO_KEY
    crude_usd = None
    try:
        r = requests.get(
            'https://api.eia.gov/v2/petroleum/pri/spt/data/'
            '?api_key=DEMO_KEY&frequency=daily&data[0]=value'
            '&facets[series][]=RWTC&sort[0][column]=period'
            '&sort[0][direction]=desc&length=1',
            headers=H, timeout=12)
        r.raise_for_status()
        rows = r.json().get('response', {}).get('data', [])
        if rows:
            crude_usd = float(rows[0]['value'])
            logger.info(f"WTI (EIA): ${crude_usd:.1f}")
    except Exception as e:
        logger.warning(f"EIA WTI: {e}")

    # Fallback WTI: Coinpaprika USO (ETF petróleo)
    if not crude_usd:
        try:
            # Usar preço de referência do crude via open data
            r = requests.get(
                'https://api.coinpaprika.com/v1/global',
                headers=H, timeout=8)
            # Coinpaprika não tem WTI — usar valor do cache se existir
            cached = cache_get('prices')
            if cached and cached.get('crude_usd'):
                crude_usd = cached['crude_usd']
                logger.info(f"WTI (cache): ${crude_usd:.1f}")
        except Exception as e:
            logger.warning(f"WTI fallback: {e}")

    if crude_usd and result['btc_usd']:
        result['crude_usd'] = crude_usd
        result['crude_btc'] = round(crude_usd / result['btc_usd'], 6)

    result['updated'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    return result


# ── NOTÍCIAS ──────────────────────────────────────────────────────────────────
BULLISH = ["bull","surge","rally","high","gain","up","rise","record","ath","buy","halving","adoption","etf","inflow"]
BEARISH = ["bear","crash","drop","down","fall","low","sell","dump","fear","hack","ban","risk","outflow","decline"]

def classify(title):
    t = title.lower()
    b = sum(1 for w in BULLISH if w in t)
    n = sum(1 for w in BEARISH if w in t)
    return 'bullish' if b > n else 'bearish' if n > b else 'neutral'

def load_sources():
    if not SOURCES_FILE.exists():
        return ['https://www.coindesk.com/arc/outboundfeeds/rss/',
                'https://cointelegraph.com/rss']
    sources = []
    for line in SOURCES_FILE.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        # Ignorar instâncias Nitter (frequentemente offline)
        if 'nitter' in line: continue
        sources.append(line)
    return sources

def fetch_news():
    H = {'User-Agent': 'Mozilla/5.0 (compatible; BitcoinIntelligence/1.0)'}
    sources = load_sources()
    items = []
    for url in sources[:8]:
        if len(items) >= 20: break
        try:
            resp = requests.get(url, headers=H, timeout=8)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            name = feed.feed.get('title', url.split('/')[2]) if feed.feed else url.split('/')[2]
            for entry in feed.entries[:3]:
                title = entry.get('title', '').strip()
                if not title: continue
                pub = entry.get('published', '')
                date_str = pub[:16] if pub else '--'
                try:
                    import email.utils
                    dt = email.utils.parsedate_to_datetime(pub)
                    date_str = dt.strftime('%d %b %H:%M')
                except Exception: pass
                items.append({
                    'title':     title,
                    'source':    name,
                    'date':      date_str,
                    'sentiment': classify(title),
                    'link':      entry.get('link', ''),
                })
        except Exception as e:
            logger.warning(f"Feed {url}: {e}")

    if not items:
        items = [{'title': 'Sem notícias disponíveis', 'source': 'Sistema',
                  'date': '--', 'sentiment': 'neutral', 'link': ''}]
    return {'items': items, 'updated': datetime.now().strftime('%H:%M')}


# ── GROQ ──────────────────────────────────────────────────────────────────────
def call_groq(system_prompt, messages, max_tokens=500):
    if not GROQ_API_KEY:
        raise RuntimeError('GROQ_API_KEY não configurada no Render → Environment.')
    payload = [{'role': 'system', 'content': system_prompt}]
    for m in messages:
        payload.append({'role': 'user' if m['role'] == 'user' else 'assistant',
                        'content': m['content']})
    resp = requests.post(
        GROQ_API_URL,
        headers={'Authorization': f'Bearer {GROQ_API_KEY}', 'Content-Type': 'application/json'},
        json={'model': GROQ_MODEL, 'messages': payload,
              'max_tokens': max_tokens, 'temperature': 0.4},
        timeout=30)
    if resp.status_code == 401: raise RuntimeError('GROQ_API_KEY inválida.')
    if resp.status_code == 429: raise RuntimeError('Limite Groq atingido (25k tokens/dia). Tenta amanhã.')
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content'].strip()

def build_system(snapshot=None):
    now = datetime.now().strftime('%A, %d de %B de %Y às %H:%M')
    if snapshot:
        s = snapshot
        dados = (
            f"Dados em tempo real:\n"
            f"- BTC/USD: {s.get('price','--')} (1h: {s.get('change1h','--')} | 24h: {s.get('change','--')})\n"
            f"- Dominância: {s.get('dom','--')} | Fear&Greed: {s.get('fg','--')} {s.get('fgLabel','')}\n"
            f"- ETH/BTC: {s.get('ethBtc','--')} | Ouro/BTC: {s.get('goldBtc','--')} | WTI/BTC: {s.get('crudeBtc','--')}\n"
            f"- Ciclo pós-halving: {s.get('halvDays','--')} ({s.get('halvPct','--')})\n"
            f"- Editorial: \"{s.get('editorial','--')}\""
        )
    else:
        p  = cache_get('prices') or {}
        fg = cache_get('feargreed') or {}
        ch24 = p.get('btc_change_24h'); ch1h = p.get('btc_change_1h')
        cr = p.get('crude_btc'); dom = p.get('dominance')
        dados = (
            f"Dados em tempo real:\n"
            f"- BTC/USD: ${p.get('btc_usd','--')} "
            f"(1h: {('%.2f%%'%ch1h) if ch1h is not None else '--'} | "
            f"24h: {('%.2f%%'%ch24) if ch24 is not None else '--'})\n"
            f"- Dominância: {('%.1f%%'%dom) if dom is not None else '--'} | "
            f"Fear&Greed: {fg.get('value','--')} {fg.get('classification','')}\n"
            f"- ETH/BTC: {p.get('eth_btc','--')} | Ouro/BTC: {p.get('gold_btc','--')} | "
            f"WTI/BTC: {('%.5f BTC'%cr) if cr is not None else '--'}"
        )
    top_q = db_top(3)
    mem = ('\nTópicos mais perguntados: ' + ' | '.join(f'"{q}"({c}x)' for q,c in top_q)) if top_q else ''
    return (
        "És um agente especializado em análise de mercados financeiros com foco em Bitcoin. "
        "Respondes SEMPRE em português de Portugal (PT-PT), de forma clara e directa.\n\n"
        f"Data: {now}\n\n{dados}{mem}\n\n"
        "Sê conciso (3-5 frases). Sem emojis. Nada é conselho financeiro directo."
    )


# ── FEAR & GREED ──────────────────────────────────────────────────────────────
def fetch_feargreed():
    try:
        r = requests.get('https://api.alternative.me/fng/?limit=7', timeout=10)
        r.raise_for_status(); data = r.json().get('data', [])
        if not data: return {'value': '50', 'yesterday': '50', 'last_week': '50'}
        return {
            'value':          data[0]['value'],
            'classification': data[0].get('value_classification', ''),
            'yesterday':      data[1]['value'] if len(data) > 1 else data[0]['value'],
            'last_week':      data[6]['value'] if len(data) > 6 else data[0]['value'],
        }
    except Exception as e:
        logger.warning(f"FearGreed: {e}")
        return {'value': '50', 'yesterday': '50', 'last_week': '50'}


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    try:
        return HTML_FILE.read_text(encoding='utf-8'), 200, {'Content-Type': 'text/html; charset=utf-8'}
    except FileNotFoundError:
        return "<h1>INDEX.HTML não encontrado</h1>", 404

@app.route('/data.json')
def data_json():
    if not DATA_FILE.exists(): return jsonify({'error': 'data.json não encontrado'}), 404
    return app.response_class(DATA_FILE.read_text(encoding='utf-8'), mimetype='application/json')

@app.route('/sw.js')
def service_worker():
    return send_from_directory(str(BASE_DIR), 'sw.js', mimetype='application/javascript')

@app.route('/manifest.json')
def manifest():
    return send_from_directory(str(BASE_DIR), 'manifest.json', mimetype='application/manifest+json')

@app.route('/icons/<path:filename>')
def icons(filename):
    return send_from_directory(str(BASE_DIR / 'icons'), filename)

@app.route('/api/data')
def api_data():
    return jsonify(get_dashboard_data()), 200

@app.route('/api/prices')
def api_prices():
    data = cache_get('prices')
    if data is None: data = fetch_prices(); cache_set('prices', data)
    return jsonify(data), 200

@app.route('/api/feargreed')
def api_feargreed():
    data = cache_get('feargreed')
    if data is None: data = fetch_feargreed(); cache_set('feargreed', data)
    return jsonify(data), 200

@app.route('/api/news')
def api_news():
    data = cache_get('news')
    if data is None: data = fetch_news(); cache_set('news', data)
    return jsonify(data), 200

@app.route('/api/btc-tick')
def api_btc_tick():
    """Ticker rápido BTC — usa cache de /api/prices (sem chamada extra)."""
    cached = cache_get('prices')
    if cached and cached.get('btc_usd'):
        return jsonify({
            'btc_usd':       cached['btc_usd'],
            'btc_change_24h': cached.get('btc_change_24h'),
            'btc_change_1h':  cached.get('btc_change_1h'),
            'from_cache': True,
        }), 200
    # Cache vazia — buscar só o preço spot rapidamente
    try:
        r = requests.get('https://api.coinbase.com/v2/prices/BTC-USD/spot', timeout=5)
        r.raise_for_status()
        return jsonify({'btc_usd': float(r.json()['data']['amount']), 'btc_change_24h': None}), 200
    except Exception:
        return jsonify({'error': 'unavailable'}), 502

@app.route('/api/agent-summary')
def api_agent_summary():
    cached = cache_get('agent_summary')
    if cached: return jsonify(cached), 200
    if not GROQ_API_KEY:
        data = get_dashboard_data()
        return jsonify({'summary': data.get('editorial', 'GROQ_API_KEY não configurada.'),
                        'generated_at': data.get('updated', '--')}), 200
    try:
        summary = call_groq(
            build_system(),
            [{'role': 'user', 'content':
              'Gera um resumo de 3-4 frases do estado actual do mercado Bitcoin. '
              'Usa os dados em tempo real. Em português de Portugal.'}],
            max_tokens=350)
        result = {'summary': summary, 'generated_at': datetime.now().strftime('%H:%M')}
        cache_set('agent_summary', result)
        return jsonify(result), 200
    except RuntimeError as e:
        return jsonify({'summary': str(e), 'generated_at': '--'}), 503
    except Exception as e:
        logger.error(f"Agent summary: {e}")
        return jsonify({'summary': 'Resumo indisponível.', 'generated_at': '--'}), 500

@app.route('/api/agent-chat', methods=['POST'])
def api_agent_chat():
    if not GROQ_API_KEY:
        return jsonify({'reply': 'Configura a variável GROQ_API_KEY no Render → Environment.'}), 200
    body     = request.get_json(force=True, silent=True) or {}
    messages = body.get('messages', [])
    snapshot = body.get('snapshot', {})
    sid      = body.get('session_id') or \
               hashlib.md5((request.remote_addr or 'anon').encode()).hexdigest()[:12]
    if not messages:
        return jsonify({'reply': 'Sem mensagens.'}), 400
    last = next((m['content'] for m in reversed(messages) if m['role'] == 'user'), None)
    if last: db_save(sid, 'user', last); db_track(last)
    full = db_history(sid, 10) + messages[-3:]
    try:
        reply = call_groq(build_system(snapshot), full, max_tokens=500)
        db_save(sid, 'agent', reply)
        return jsonify({'reply': reply, 'session_id': sid}), 200
    except RuntimeError as e:
        return jsonify({'reply': str(e)}), 503
    except Exception as e:
        logger.error(f"Agent chat: {e}")
        return jsonify({'reply': 'Erro. Tenta novamente.'}), 500

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat(),
                    'data_available': DATA_FILE.exists()}), 200

@app.route('/api/ping')
def api_ping():
    """Keep-alive para UptimeRobot — refresca dados se tiverem mais de 30 min."""
    data = get_dashboard_data()
    updated = data.get('updated', '')
    needs_refresh = False
    try:
        last = datetime.strptime(updated, '%d/%m/%Y %H:%M')
        needs_refresh = (datetime.now() - last).total_seconds() > 1800
    except Exception:
        needs_refresh = True
    if needs_refresh:
        import subprocess, sys
        try:
            gen = BASE_DIR / 'generate_data.py'
            if gen.exists():
                subprocess.Popen([sys.executable, str(gen)], cwd=str(BASE_DIR),
                                 env={**os.environ})
        except Exception: pass
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat(),
                    'data_updated': updated, 'refresh_triggered': needs_refresh}), 200

@app.route('/api/config')
def api_config():
    return jsonify({'groq_key_set': bool(GROQ_API_KEY), 'groq_model': GROQ_MODEL,
                    'data_file_exists': DATA_FILE.exists(),
                    'db': db_stats()}), 200

@app.route('/api/stats')
def api_stats():
    return jsonify({**db_stats(), 'top_questions': db_top(10)}), 200

@app.errorhandler(404)
def not_found(e):   return jsonify({'error': 'Endpoint não encontrado'}), 404
@app.errorhandler(500)
def server_error(e): return jsonify({'error': 'Erro interno do servidor'}), 500


# ── ARRANQUE ──────────────────────────────────────────────────────────────────
def warm_cache():
    logger.info("A pré-carregar cache...")
    cache_set('prices',    fetch_prices())
    cache_set('news',      fetch_news())
    cache_set('feargreed', fetch_feargreed())
    logger.info("Cache pronta.")

db_init()
threading.Thread(target=warm_cache, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Bitcoin Intelligence · porta {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
