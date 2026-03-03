"""
server.py — Bitcoin Intelligence · Render + Groq
pip install flask requests feedparser gunicorn
"""

import json, time, threading, sqlite3, hashlib, os, re
from datetime import datetime
from pathlib import Path

import requests, feedparser
from flask import Flask, jsonify, send_from_directory, request

BASE_DIR = Path(__file__).parent
DB_PATH  = Path(os.environ.get('RENDER_DISK_PATH', str(BASE_DIR))) / 'memory.db'

app = Flask(__name__, static_folder=str(BASE_DIR))

# ── GROQ ──────────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL   = 'llama-3.1-8b-instant'
GROQ_URL     = 'https://api.groq.com/openai/v1/chat/completions'

def call_groq(system_prompt, messages, max_tokens=500):
    if not GROQ_API_KEY:
        raise RuntimeError('GROQ_API_KEY não configurada no Render → Environment.')
    payload = [{'role': 'system', 'content': system_prompt}]
    for m in messages:
        payload.append({'role': 'user' if m['role']=='user' else 'assistant',
                        'content': m['content']})
    resp = requests.post(GROQ_URL,
        headers={'Authorization': f'Bearer {GROQ_API_KEY}', 'Content-Type': 'application/json'},
        json={'model': GROQ_MODEL, 'messages': payload,
              'max_tokens': max_tokens, 'temperature': 0.4},
        timeout=30)
    if resp.status_code == 401: raise RuntimeError('GROQ_API_KEY inválida.')
    if resp.status_code == 429: raise RuntimeError('Limite Groq atingido (25k tokens/dia).')
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content'].strip()


# ── BASE DE DADOS ─────────────────────────────────────────────────────────────
def db_init():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL, role TEXT NOT NULL,
            content TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sess ON conversations(session_id);
        CREATE TABLE IF NOT EXISTS common_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL UNIQUE, count INTEGER DEFAULT 1, last_seen TEXT NOT NULL
        );
    """)
    con.commit(); con.close()

def db_save(sid, role, content):
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO conversations (session_id,role,content,created_at) VALUES (?,?,?,?)",
                (sid, role, content, datetime.now().isoformat()))
    con.commit(); con.close()

def db_history(sid, limit=10):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT role,content FROM conversations WHERE session_id=? ORDER BY id DESC LIMIT ?",
        (sid, limit)).fetchall()
    con.close()
    return [{'role': r, 'content': c} for r, c in reversed(rows)]

def db_track(q):
    qn = q.strip().lower()[:200]
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("INSERT INTO common_questions (question,count,last_seen) VALUES (?,1,?) "
                    "ON CONFLICT(question) DO UPDATE SET count=count+1,last_seen=excluded.last_seen",
                    (qn, datetime.now().isoformat()))
    except:
        con.execute("UPDATE common_questions SET count=count+1,last_seen=? WHERE question=?",
                    (datetime.now().isoformat(), qn))
    con.commit(); con.close()

def db_top(n=5):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT question,count FROM common_questions ORDER BY count DESC LIMIT ?",
                       (n,)).fetchall()
    con.close(); return rows

def db_stats():
    con = sqlite3.connect(DB_PATH)
    msgs = con.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
    sess = con.execute("SELECT COUNT(DISTINCT session_id) FROM conversations").fetchone()[0]
    qs   = con.execute("SELECT COUNT(*) FROM common_questions").fetchone()[0]
    con.close(); return {'messages': msgs, 'sessions': sess, 'unique_questions': qs}


# ── CACHE ─────────────────────────────────────────────────────────────────────
_cache = {}
CACHE_TTL = {'prices': 60, 'news': 600, 'feargreed': 3600, 'agent_summary': 300}

def cache_get(key):
    e = _cache.get(key)
    if e and (time.time() - e['ts']) < CACHE_TTL.get(key, 60): return e['data']
    return None

def cache_set(key, data): _cache[key] = {'data': data, 'ts': time.time()}


# ── SYSTEM PROMPT ─────────────────────────────────────────────────────────────
def build_system(snapshot=None):
    now = datetime.now().strftime('%A, %d de %B de %Y às %H:%M')
    if snapshot:
        s = snapshot
        dados = (f"Dados em tempo real:\n"
                 f"- BTC/USD: {s.get('price','--')} (1h: {s.get('change1h','--')} | 24h: {s.get('change24h','--')})\n"
                 f"- Dominância: {s.get('dom','--')} | Fear&Greed: {s.get('fg','--')} {s.get('fgLabel','')}\n"
                 f"- ETH/BTC: {s.get('ethBtc','--')} | Ouro/BTC: {s.get('goldBtc','--')} | WTI/BTC: {s.get('crudeBtc','--')}\n"
                 f"- Ciclo pós-halving: {s.get('halvDays','--')} ({s.get('halvPct','--')})")
    else:
        p = cache_get('prices') or {}; fg = cache_get('feargreed') or {}
        ch24=p.get('btc_change_24h'); ch1h=p.get('btc_change_1h')
        cr=p.get('crude_btc'); dom=p.get('dominance')
        dados = (f"Dados em tempo real:\n"
                 f"- BTC/USD: ${p.get('btc_usd','--')} "
                 f"(1h: {('%.2f%%'%ch1h) if ch1h is not None else '--'} | "
                 f"24h: {('%.2f%%'%ch24) if ch24 is not None else '--'})\n"
                 f"- Dominância: {('%.1f%%'%dom) if dom is not None else '--'} | "
                 f"Fear&Greed: {fg.get('value','--')} {fg.get('classification','')}\n"
                 f"- ETH/BTC: {p.get('eth_btc','--')} | Ouro/BTC: {p.get('gold_btc','--')} | "
                 f"WTI/BTC: {('%.5f BTC'%cr) if cr is not None else '--'}")
    top_q = db_top(3)
    mem = ('\nTópicos mais perguntados: ' + ' | '.join(f'"{q}"({c}x)' for q,c in top_q)) if top_q else ''
    return (
        "És um agente especializado em análise de mercados financeiros com foco em Bitcoin. "
        "Respondes SEMPRE em português de Portugal (PT-PT), de forma clara e directa.\n\n"
        f"Data: {now}\n\n{dados}{mem}\n\n"
        "Sê conciso (3-5 frases). Sem emojis. Nada é conselho financeiro directo."
    )


# ── FETCH PREÇOS ──────────────────────────────────────────────────────────────
def stooq_val(ticker, lo, hi):
    """Busca valor numérico do Stooq para um ticker."""
    r = requests.get(f'https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv',
                     headers={'User-Agent': 'Mozilla/5.0'}, timeout=12)
    r.raise_for_status()
    for line in reversed([l.strip() for l in r.text.strip().split('\n') if l.strip()][1:]):
        for col in reversed(line.split(',')):
            try:
                v = float(col)
                if lo < v < hi: return v
            except: continue
    return None

def fetch_prices():
    result = {
        'btc_usd': None, 'btc_change_24h': None, 'btc_change_1h': None,
        'eth_btc': None, 'gold_btc': None, 'dominance': None,
        'dxy_approx': None, 'crude_usd': None, 'crude_btc': None, 'source': [],
    }
    H = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

    # ── Kraken: BTC, ETH/BTC, variações ──────────────────────────────────────
    try:
        r = requests.get('https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHXBT',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        # BTC preço
        for k in ['XXBTZUSD', 'XBTUSD']:
            if k in d:
                result['btc_usd'] = float(d[k]['c'][0]); break
        # ETH/BTC
        for k in ['XETHXXBT', 'ETHXBT']:
            if k in d:
                result['eth_btc'] = float(d[k]['c'][0]); break
        result['source'].append('Kraken')
        print(f"BTC: ${result['btc_usd']:,.0f} | ETH/BTC: {result['eth_btc']}")
    except Exception as e: print(f"Kraken ticker: {e}")

    # Fallback BTC: Coinbase
    if result['btc_usd'] is None:
        try:
            r = requests.get('https://api.coinbase.com/v2/prices/BTC-USD/spot', headers=H, timeout=8)
            r.raise_for_status()
            result['btc_usd'] = float(r.json()['data']['amount'])
            result['source'].append('Coinbase')
        except Exception as e: print(f"Coinbase: {e}")

    # ── Kraken OHLC: variação 24h ─────────────────────────────────────────────
    try:
        r = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next((k for k in d if k != 'last'), None)
        if key and len(d[key]) >= 2:
            o = float(d[key][-2][1]); c = float(d[key][-1][4])
            if o > 0: result['btc_change_24h'] = round((c - o) / o * 100, 3)
            print(f"BTC 24h: {result['btc_change_24h']:.2f}%")
    except Exception as e: print(f"Kraken 24h: {e}")

    # ── Kraken OHLC: variação 1h ──────────────────────────────────────────────
    try:
        r = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next((k for k in d if k != 'last'), None)
        if key and len(d[key]) >= 2:
            o = float(d[key][-2][1]); c = float(d[key][-1][4])
            if o > 0: result['btc_change_1h'] = round((c - o) / o * 100, 3)
            print(f"BTC 1h: {result['btc_change_1h']:.2f}%")
    except Exception as e: print(f"Kraken 1h: {e}")

    # ── Dominância: Coinpaprika ───────────────────────────────────────────────
    try:
        r = requests.get('https://api.coinpaprika.com/v1/global', headers=H, timeout=10)
        r.raise_for_status()
        result['dominance'] = r.json().get('bitcoin_dominance_percentage')
        print(f"Dominância: {result['dominance']:.1f}%")
    except Exception as e: print(f"Coinpaprika: {e}")

    # ── Ouro: Kraken XAUUSD ───────────────────────────────────────────────────
    gold_usd = None
    try:
        r = requests.get('https://api.kraken.com/0/public/Ticker?pair=XAUUSD',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next(iter(d), None)
        if key: gold_usd = float(d[key]['c'][0])
        print(f"Ouro (Kraken): ${gold_usd:.0f}")
    except Exception as e: print(f"Kraken ouro: {e}")

    # Fallback ouro: Stooq GC.F
    if not gold_usd:
        try:
            gold_usd = stooq_val('gc.f', 1500, 5000)
            if gold_usd: print(f"Ouro (Stooq): ${gold_usd:.0f}")
        except Exception as e: print(f"Stooq ouro: {e}")

    if gold_usd and result['btc_usd']:
        result['gold_btc'] = round(gold_usd / result['btc_usd'], 6)
        print(f"Ouro/BTC: {result['gold_btc']}")

    # ── Petróleo WTI: Stooq CL.F ─────────────────────────────────────────────
    try:
        crude_usd = stooq_val('cl.f', 40, 200)
        if crude_usd:
            result['crude_usd'] = crude_usd
            if result['btc_usd']:
                result['crude_btc'] = round(crude_usd / result['btc_usd'], 6)
            print(f"WTI: ${crude_usd:.1f} = {result['crude_btc']} BTC")
        else:
            print("WTI: N/D (mercado fechado)")
    except Exception as e: print(f"Stooq WTI: {e}")

    result['updated'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    return result


# ── NOTÍCIAS ──────────────────────────────────────────────────────────────────
def load_sources():
    path = BASE_DIR / 'sources.txt'
    if not path.exists():
        return [('https://www.coindesk.com/arc/outboundfeeds/rss/', 'CoinDesk'),
                ('https://cointelegraph.com/rss', 'CoinTelegraph')]
    sources = []
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        try:
            from urllib.parse import urlparse
            name = urlparse(line).netloc.replace('www.','').split('.')[0].capitalize()
        except: name = 'Feed'
        sources.append((line, name))
    return sources

def translate_titles(articles):
    if not articles or not GROQ_API_KEY: return articles
    numbered = '\n'.join(f"{i+1}. {a['title']}" for i,a in enumerate(articles))
    try:
        result = call_groq(
            "Translate each headline to European Portuguese (PT-PT). "
            "Reply ONLY with numbered lines: 1. Tradução",
            [{'role':'user','content': numbered}], max_tokens=800)
        translated = [re.sub(r'^\d+[\.):\s]+','',l.strip()).strip()
                      for l in result.splitlines() if l.strip() and len(l.strip())>3]
        if len(translated) >= len(articles):
            for i,a in enumerate(articles): a['title'] = translated[i]
    except Exception as e: print(f"Tradução: {e}")
    return articles

def fetch_news():
    sources, articles = load_sources(), []
    for url, source in sources:
        if len(articles) >= 12: break
        try:
            feed = feedparser.parse(url); count = 0
            for entry in feed.entries:
                if count >= 4 or len(articles) >= 12: break
                title = entry.get('title','').strip()
                link  = entry.get('link','')
                pub   = entry.get('published','')
                date_str = ''
                if pub:
                    try:
                        import email.utils
                        dt = email.utils.parsedate_to_datetime(pub)
                        date_str = dt.strftime('%d %b %H:%M')
                    except: date_str = pub[:16]
                tl = title.lower()
                if any(w in tl for w in ['bull','surge','rally','gain','high','ath','rise','pump','etf','record','soar']): s='bullish'
                elif any(w in tl for w in ['bear','crash','drop','fall','fear','ban','hack','sell','decline','warning','risk','plunge']): s='bearish'
                else: s='neutral'
                articles.append({'title':title,'link':link,'date':date_str,'source':source,'sentiment':s})
                count += 1
        except: pass
    return translate_titles(articles)

def fetch_feargreed():
    try:
        r = requests.get('https://api.alternative.me/fng/?limit=7&format=json', timeout=8)
        r.raise_for_status(); data = r.json().get('data',[])
        if not data: return {}
        return {'value': data[0].get('value'), 'classification': data[0].get('value_classification'),
                'yesterday': data[1].get('value') if len(data)>1 else None,
                'last_week': data[6].get('value') if len(data)>6 else None}
    except: return {}


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.route('/api/prices')
def api_prices():
    data = cache_get('prices')
    if data is None: data = fetch_prices(); cache_set('prices', data)
    return jsonify(data)

@app.route('/api/news')
def api_news():
    data = cache_get('news')
    if data is None: data = fetch_news(); cache_set('news', data)
    return jsonify(data)

@app.route('/api/feargreed')
def api_feargreed():
    data = cache_get('feargreed')
    if data is None: data = fetch_feargreed(); cache_set('feargreed', data)
    return jsonify(data)

@app.route('/api/agent-summary')
def api_agent_summary():
    cached = cache_get('agent_summary')
    if cached: return jsonify(cached)
    try:
        summary = call_groq(build_system(),
            [{'role':'user','content':
              'Gera um resumo de 3-4 frases do estado actual do mercado Bitcoin. '
              'Usa os dados em tempo real. Em português de Portugal.'}], max_tokens=350)
        result = {'summary': summary, 'generated_at': datetime.now().strftime('%H:%M')}
        cache_set('agent_summary', result); return jsonify(result)
    except RuntimeError as e: return jsonify({'summary': str(e), 'generated_at': '--'}), 503
    except Exception as e:
        print(f"Agent summary: {e}")
        return jsonify({'summary': 'Resumo indisponível.', 'generated_at': '--'}), 500

@app.route('/api/agent-chat', methods=['POST'])
def api_agent_chat():
    body     = request.get_json(force=True, silent=True) or {}
    messages = body.get('messages', [])
    snapshot = body.get('snapshot', None)
    sid      = body.get('session_id') or \
               hashlib.md5((request.remote_addr or 'anon').encode()).hexdigest()[:12]
    if not messages: return jsonify({'reply':'Sem mensagens.'}), 400
    last = next((m['content'] for m in reversed(messages) if m['role']=='user'), None)
    if last: db_save(sid,'user',last); db_track(last)
    full = db_history(sid, 10) + messages[-3:]
    try:
        reply = call_groq(build_system(snapshot), full, max_tokens=500)
        db_save(sid, 'agent', reply)
        return jsonify({'reply': reply, 'session_id': sid})
    except RuntimeError as e: return jsonify({'reply': str(e)}), 503
    except Exception as e:
        print(f"Agent chat: {e}")
        return jsonify({'reply': 'Erro. Tenta novamente.'}), 500

@app.route('/api/stats')
def api_stats():
    return jsonify({**db_stats(), 'top_questions': db_top(10)})

@app.route('/api/status')
def api_status():
    return jsonify({'status':'ok', 'time': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                    'groq_configured': bool(GROQ_API_KEY), 'groq_model': GROQ_MODEL,
                    'db': db_stats()})

@app.route('/data.json')
def data_json():
    path = BASE_DIR / 'data.json'
    if not path.exists(): return jsonify({'error':'data.json não encontrado'}), 404
    return app.response_class(path.read_text(encoding='utf-8'), mimetype='application/json')

@app.route('/sw.js')
def service_worker():
    return send_from_directory(str(BASE_DIR), 'sw.js', mimetype='application/javascript')

@app.route('/manifest.json')
def manifest():
    return send_from_directory(str(BASE_DIR), 'manifest.json', mimetype='application/manifest+json')

@app.route('/icons/<path:filename>')
def icons(filename):
    return send_from_directory(str(BASE_DIR / 'icons'), filename)

@app.route('/')
def index():
    return send_from_directory(str(BASE_DIR), 'INDEX.HTML')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(str(BASE_DIR), filename)


# ── ARRANQUE ──────────────────────────────────────────────────────────────────
def warm_cache():
    print("A pré-carregar cache...")
    cache_set('prices',    fetch_prices())
    cache_set('news',      fetch_news())
    cache_set('feargreed', fetch_feargreed())
    print("Cache pronta.")

db_init()
threading.Thread(target=warm_cache, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"Bitcoin Intelligence · porta {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
"""
server.py — Bitcoin Intelligence · Render + Groq
pip install flask requests feedparser
"""

import json, time, threading, sqlite3, hashlib, os
from datetime import datetime
from pathlib import Path

import requests, feedparser
from flask import Flask, jsonify, send_from_directory, request

BASE_DIR = Path(__file__).parent
DB_PATH  = Path(os.environ.get('RENDER_DISK_PATH', str(BASE_DIR))) / 'memory.db'

app = Flask(__name__, static_folder=str(BASE_DIR))

# ── GROQ API ──────────────────────────────────────────────────────────────────
# Chave lida da variável de ambiente GROQ_API_KEY (definida no Render)
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL   = 'llama-3.1-8b-instant'   # gratuito, rápido
GROQ_URL     = 'https://api.groq.com/openai/v1/chat/completions'

def call_groq(system_prompt, messages, max_tokens=500):
    if not GROQ_API_KEY:
        raise RuntimeError(
            'GROQ_API_KEY não configurada. '
            'Cria conta em console.groq.com → API Keys → copia a chave → '
            'Render Dashboard → Environment → adiciona GROQ_API_KEY'
        )
    # Monta mensagens no formato OpenAI
    payload_messages = [{'role': 'system', 'content': system_prompt}]
    for m in messages:
        payload_messages.append({
            'role':    'user'      if m['role'] == 'user' else 'assistant',
            'content': m['content']
        })
    try:
        resp = requests.post(
            GROQ_URL,
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type':  'application/json',
            },
            json={
                'model':       GROQ_MODEL,
                'messages':    payload_messages,
                'max_tokens':  max_tokens,
                'temperature': 0.4,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()['choices'][0]['message']['content'].strip()
    except requests.exceptions.HTTPError as e:
        if resp.status_code == 401:
            raise RuntimeError('GROQ_API_KEY inválida. Verifica em console.groq.com')
        if resp.status_code == 429:
            raise RuntimeError('Limite Groq atingido (25k tokens/dia). Tenta amanhã.')
        raise RuntimeError(f'Groq erro {resp.status_code}: {e}')
    except requests.exceptions.Timeout:
        raise RuntimeError('Groq timeout. Tenta novamente.')


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
    print(f"DB: {DB_PATH}")

def db_save(session_id, role, content):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO conversations (session_id,role,content,created_at) VALUES (?,?,?,?)",
        (session_id, role, content, datetime.now().isoformat())
    )
    con.commit(); con.close()

def db_history(session_id, limit=10):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT role,content FROM conversations WHERE session_id=? ORDER BY id DESC LIMIT ?",
        (session_id, limit)
    ).fetchall()
    con.close()
    return [{'role': r, 'content': c} for r, c in reversed(rows)]

def db_track(question):
    q = question.strip().lower()[:200]
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            "INSERT INTO common_questions (question,count,last_seen) VALUES (?,1,?) "
            "ON CONFLICT(question) DO UPDATE SET count=count+1, last_seen=excluded.last_seen",
            (q, datetime.now().isoformat())
        )
    except Exception:
        con.execute(
            "UPDATE common_questions SET count=count+1, last_seen=? WHERE question=?",
            (datetime.now().isoformat(), q)
        )
    con.commit(); con.close()

def db_top(n=5):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT question,count FROM common_questions ORDER BY count DESC LIMIT ?", (n,)
    ).fetchall()
    con.close()
    return rows

def db_stats():
    con = sqlite3.connect(DB_PATH)
    msgs = con.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
    sess = con.execute("SELECT COUNT(DISTINCT session_id) FROM conversations").fetchone()[0]
    qs   = con.execute("SELECT COUNT(*) FROM common_questions").fetchone()[0]
    con.close()
    return {'messages': msgs, 'sessions': sess, 'unique_questions': qs}


# ── CACHE ─────────────────────────────────────────────────────────────────────
_cache = {}
CACHE_TTL = {'prices': 60, 'news': 600, 'feargreed': 3600, 'agent_summary': 300}

def cache_get(key):
    e = _cache.get(key)
    if e and (time.time() - e['ts']) < CACHE_TTL.get(key, 60): return e['data']
    return None

def cache_set(key, data):
    _cache[key] = {'data': data, 'ts': time.time()}


# ── SYSTEM PROMPT ─────────────────────────────────────────────────────────────
def build_system(snapshot=None):
    now = datetime.now().strftime('%A, %d de %B de %Y às %H:%M')

    if snapshot:
        s = snapshot
        dados = (
            f"Dados em tempo real:\n"
            f"- BTC/USD: {s.get('price','--')} "
            f"(1h: {s.get('change1h','--')} | 24h: {s.get('change24h','--')})\n"
            f"- Dominância: {s.get('dom','--')} | "
            f"Fear & Greed: {s.get('fg','--')} {s.get('fgLabel','')}\n"
            f"- ETH/BTC: {s.get('ethBtc','--')} | "
            f"Ouro/BTC: {s.get('goldBtc','--')} | "
            f"WTI/BTC: {s.get('crudeBtc','--')}\n"
            f"- Ciclo pós-halving: {s.get('halvDays','--')} ({s.get('halvPct','--')})\n"
            f"- Editorial: \"{s.get('editorial','--')}\""
        )
    else:
        p   = cache_get('prices') or {}
        fg  = cache_get('feargreed') or {}
        ch24 = p.get('btc_change_24h')
        ch1h = p.get('btc_change_1h')
        cr   = p.get('crude_btc')
        dom  = p.get('dominance')
        dados = (
            f"Dados em tempo real:\n"
            f"- BTC/USD: ${p.get('btc_usd','--')} "
            f"(1h: {('%.2f%%'%ch1h) if ch1h is not None else '--'} | "
            f"24h: {('%.2f%%'%ch24) if ch24 is not None else '--'})\n"
            f"- Dominância: {('%.1f%%'%dom) if dom is not None else '--'} | "
            f"Fear & Greed: {fg.get('value','--')} {fg.get('classification','')}\n"
            f"- ETH/BTC: {p.get('eth_btc','--')} | "
            f"Ouro/BTC: {p.get('gold_btc','--')} | "
            f"WTI/BTC: {('%.5f BTC'%cr) if cr is not None else '--'}"
        )

    # Contexto aprendido das perguntas mais frequentes
    top_q = db_top(3)
    memoria = ''
    if top_q:
        qs = ' | '.join(f'"{q}"({c}x)' for q, c in top_q)
        memoria = f'\nTópicos mais perguntados pelos utilizadores: {qs}'

    return (
        "És um agente especializado em análise de mercados financeiros com foco em Bitcoin. "
        "Respondes SEMPRE em português de Portugal (PT-PT), de forma clara, precisa e directa.\n\n"
        f"Data actual: {now}\n\n{dados}{memoria}\n\n"
        "Competências: análise técnica, on-chain, macroeconomia, ciclos de halving, DCA. "
        "Sê conciso (3-5 frases). Sem emojis. Nada é conselho financeiro directo."
    )


# ── FETCH PREÇOS ──────────────────────────────────────────────────────────────
def fetch_prices():
    result = {
        'btc_usd':None,'btc_change_24h':None,'btc_change_1h':None,
        'eth_btc':None,'gold_btc':None,'dominance':None,
        'dxy_approx':None,'crude_usd':None,'crude_btc':None,'source':[],
    }

    H = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

    # ── 1. BTC + ETH + variações — Kraken (sem bloqueio em cloud) ────────────
    try:
        r = requests.get(
            'https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD,ETHXBT',
            headers=H, timeout=10)
        r.raise_for_status(); d = r.json()['result']

        btc = d.get('XXBTZUSD', d.get('XBTUSD', {}))
        if btc:
            result['btc_usd'] = float(btc['c'][0])
            result['source'].append('Kraken')
            print(f"BTC: ${result['btc_usd']:,.0f}")

        eth = d.get('XETHXXBT', d.get('ETHXBT', {}))
        if eth:
            result['eth_btc'] = float(eth['c'][0])
            print(f"ETH/BTC: {result['eth_btc']:.5f}")
    except Exception as e: print(f"Kraken ticker: {e}")

    # ── 2. BTC variação 24h — Kraken OHLC ────────────────────────────────────
    try:
        r = requests.get(
            'https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440',
            headers=H, timeout=10)
        r.raise_for_status(); d = r.json()['result']
        key = [k for k in d if k != 'last'][0]
        candles = d[key]
        if len(candles) >= 2:
            open_24h  = float(candles[-2][1])
            close_24h = float(candles[-1][4])
            if open_24h > 0:
                result['btc_change_24h'] = round((close_24h - open_24h) / open_24h * 100, 3)
                print(f"BTC 24h: {result['btc_change_24h']:.2f}%")
    except Exception as e: print(f"Kraken 24h: {e}")

    # ── 3. BTC variação 1h — Kraken OHLC 1h ──────────────────────────────────
    try:
        r = requests.get(
            'https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60',
            headers=H, timeout=10)
        r.raise_for_status(); d = r.json()['result']
        key = [k for k in d if k != 'last'][0]
        candles = d[key]
        if len(candles) >= 2:
            open_1h  = float(candles[-2][1])
            close_1h = float(candles[-1][4])
            if open_1h > 0:
                result['btc_change_1h'] = round((close_1h - open_1h) / open_1h * 100, 3)
                print(f"BTC 1h: {result['btc_change_1h']:.2f}%")
    except Exception as e: print(f"Kraken 1h: {e}")

    # ── 4. Dominância — Coinpaprika (sem rate limit em cloud) ────────────────
    try:
        r = requests.get('https://api.coinpaprika.com/v1/global', headers=H, timeout=10)
        r.raise_for_status()
        result['dominance'] = r.json().get('bitcoin_dominance_percentage')
        print(f"Dominância: {result['dominance']:.1f}%")
    except Exception as e: print(f"Coinpaprika dominância: {e}")

    # ── 5. Ouro USD — metals.dev API (free tier, sem API key necessária) ─────
    # gold price via open.er-api ou fixer fallback
    gold_usd = None
    try:
        # Frankfurter não tem metais, usar metals-api alternativa
        # API pública do Gold Price API (goldapi.io tem free tier mas precisa registo)
        # Usar Open Exchange Rates via metals endpoint
        r = requests.get(
            'https://api.metals.live/v1/spot/gold',
            headers=H, timeout=10)
        r.raise_for_status()
        data = r.json()
        # resposta: [{"gold": 3300.5}] ou {"price": 3300.5}
        if isinstance(data, list) and data:
            gold_usd = float(data[0].get('gold', 0)) or None
        elif isinstance(data, dict):
            gold_usd = float(data.get('price', 0) or data.get('gold', 0)) or None
        if gold_usd:
            print(f"Ouro (metals.live): ${gold_usd:.0f}")
    except Exception as e: print(f"metals.live: {e}")

    # Fallback ouro: Stooq GC.F
    if not gold_usd:
        try:
            r = requests.get('https://stooq.com/q/l/?s=gc.f&f=sd2t2ohlcv&h&e=csv',
                             headers=H, timeout=10)
            r.raise_for_status()
            lines = [l.strip() for l in r.text.strip().split('\n') if l.strip()]
            for line in reversed(lines[1:]):
                for col in reversed(line.split(',')):
                    try:
                        v = float(col)
                        if 1500 < v < 5000: gold_usd = v; break
                    except: continue
                if gold_usd: break
            if gold_usd: print(f"Ouro (Stooq): ${gold_usd:.0f}")
        except Exception as e: print(f"Stooq ouro: {e}")

    if gold_usd and result['btc_usd']:
        result['gold_btc'] = round(gold_usd / result['btc_usd'], 6)
        print(f"Ouro/BTC: {result['gold_btc']:.6f}")

    # ── 6. Petróleo WTI — Stooq cl.f ─────────────────────────────────────────
    try:
        r = requests.get('https://stooq.com/q/l/?s=cl.f&f=sd2t2ohlcv&h&e=csv',
                         headers=H, timeout=10)
        r.raise_for_status()
        lines = [l.strip() for l in r.text.strip().split('\n') if l.strip()]
        crude_usd = None
        for line in reversed(lines[1:]):
            for col in reversed(line.split(',')):
                try:
                    v = float(col)
                    if 40 < v < 200: crude_usd = v; break
                except: continue
            if crude_usd: break
        if crude_usd:
            result['crude_usd'] = crude_usd
            if result['btc_usd']:
                result['crude_btc'] = round(crude_usd / result['btc_usd'], 6)
            print(f"WTI: ${crude_usd:.1f} = {result['crude_btc']:.6f} BTC")
        else:
            print("WTI: N/D (mercado fechado)")
    except Exception as e: print(f"Stooq WTI: {e}")

    result['updated'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    return result


# ── NOTÍCIAS ──────────────────────────────────────────────────────────────────
def load_sources():
    path = BASE_DIR / 'sources.txt'
    if not path.exists():
        return [('https://www.coindesk.com/arc/outboundfeeds/rss/','CoinDesk'),
                ('https://cointelegraph.com/rss','CoinTelegraph')]
    sources = []
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        try:
            from urllib.parse import urlparse
            name = urlparse(line).netloc.replace('www.','').split('.')[0].capitalize()
        except: name='Feed'
        sources.append((line, name))
    return sources

def translate_titles(articles):
    """Traduz títulos via Groq (muito mais rápido que Ollama)."""
    if not articles or not GROQ_API_KEY: return articles
    titles   = [a['title'] for a in articles]
    numbered = '\n'.join(f"{i+1}. {t}" for i,t in enumerate(titles))
    try:
        result = call_groq(
            "You are a translator. Translate each headline to European Portuguese (Portugal). "
            "Reply ONLY with numbered translations, one per line: 1. Tradução\n2. Tradução\n...",
            [{'role':'user','content': numbered}],
            max_tokens=800
        )
        import re
        translated = [re.sub(r'^\d+[\.):\s]+','',l.strip()).strip()
                      for l in result.splitlines() if l.strip()]
        translated = [t for t in translated if len(t)>3]
        if len(translated) >= len(articles):
            for i,a in enumerate(articles): a['title'] = translated[i]
    except Exception as e:
        print(f"Tradução Groq: {e}")
    return articles

def fetch_news():
    sources, articles = load_sources(), []
    for url, source in sources:
        if len(articles) >= 12: break
        try:
            feed = feedparser.parse(url); count = 0
            for entry in feed.entries:
                if count >= 4 or len(articles) >= 12: break
                title = entry.get('title','').strip()
                link  = entry.get('link','')
                pub   = entry.get('published','')
                date_str = ''
                if pub:
                    try:
                        import email.utils
                        dt = email.utils.parsedate_to_datetime(pub)
                        date_str = dt.strftime('%d %b %H:%M')
                    except: date_str = pub[:16]
                tl = title.lower()
                if any(w in tl for w in ['bull','surge','rally','gain','high','ath','rise','pump','inflow','adopt','etf','record','soar']): s='bullish'
                elif any(w in tl for w in ['bear','crash','drop','fall','fear','ban','hack','sell','outflow','decline','warning','risk','plunge']): s='bearish'
                else: s='neutral'
                articles.append({'title':title,'link':link,'date':date_str,'source':source,'sentiment':s})
                count += 1
        except: pass
    return translate_titles(articles)

def fetch_feargreed():
    try:
        r = requests.get('https://api.alternative.me/fng/?limit=7&format=json', timeout=8)
        r.raise_for_status(); data = r.json().get('data',[])
        if not data: return {}
        return {
            'value':data[0].get('value'), 'classification':data[0].get('value_classification'),
            'yesterday':data[1].get('value') if len(data)>1 else None,
            'last_week':data[6].get('value') if len(data)>6 else None,
        }
    except: return {}


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.route('/api/prices')
def api_prices():
    data = cache_get('prices')
    if data is None: data=fetch_prices(); cache_set('prices',data)
    return jsonify(data)

@app.route('/api/news')
def api_news():
    data = cache_get('news')
    if data is None: data=fetch_news(); cache_set('news',data)
    return jsonify(data)

@app.route('/api/feargreed')
def api_feargreed():
    data = cache_get('feargreed')
    if data is None: data=fetch_feargreed(); cache_set('feargreed',data)
    return jsonify(data)

@app.route('/api/agent-summary')
def api_agent_summary():
    cached = cache_get('agent_summary')
    if cached: return jsonify(cached)
    try:
        summary = call_groq(
            build_system(),
            [{'role':'user','content':
              'Gera um resumo de 3-4 frases do estado actual do mercado Bitcoin. '
              'Usa os dados em tempo real. Em português de Portugal.'}],
            max_tokens=350
        )
        result = {'summary': summary, 'generated_at': datetime.now().strftime('%H:%M')}
        cache_set('agent_summary', result)
        return jsonify(result)
    except RuntimeError as e:
        return jsonify({'summary': str(e), 'generated_at': '--'}), 503
    except Exception as e:
        print(f"Agent summary: {e}")
        return jsonify({'summary': 'Resumo indisponível.', 'generated_at': '--'}), 500

@app.route('/api/agent-chat', methods=['POST'])
def api_agent_chat():
    body       = request.get_json(force=True, silent=True) or {}
    messages   = body.get('messages', [])
    snapshot   = body.get('snapshot', None)
    session_id = body.get('session_id') or \
                 hashlib.md5((request.remote_addr or 'anon').encode()).hexdigest()[:12]
    if not messages:
        return jsonify({'reply':'Sem mensagens.'}), 400

    last_user = next((m['content'] for m in reversed(messages) if m['role']=='user'), None)
    if last_user:
        db_save(session_id, 'user', last_user)
        db_track(last_user)

    # Histórico guardado + últimas mensagens do cliente
    history      = db_history(session_id, limit=10)
    full_messages = history + messages[-3:]

    try:
        reply = call_groq(build_system(snapshot), full_messages, max_tokens=500)
        db_save(session_id, 'agent', reply)
        return jsonify({'reply': reply, 'session_id': session_id})
    except RuntimeError as e:
        return jsonify({'reply': str(e)}), 503
    except Exception as e:
        print(f"Agent chat: {e}")
        return jsonify({'reply': 'Erro. Tenta novamente.'}), 500

@app.route('/api/stats')
def api_stats():
    return jsonify({**db_stats(), 'top_questions': db_top(10)})

@app.route('/api/status')
def api_status():
    return jsonify({
        'status': 'ok',
        'time': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'groq_configured': bool(GROQ_API_KEY),
        'groq_model': GROQ_MODEL,
        'db': db_stats(),
    })

@app.route('/data.json')
def data_json():
    path = BASE_DIR / 'data.json'
    if not path.exists(): return jsonify({'error':'data.json não encontrado'}), 404
    return app.response_class(path.read_text(encoding='utf-8'), mimetype='application/json')

@app.route('/sw.js')
def service_worker():
    return send_from_directory(str(BASE_DIR), 'sw.js', mimetype='application/javascript')

@app.route('/manifest.json')
def manifest():
    return send_from_directory(str(BASE_DIR), 'manifest.json', mimetype='application/manifest+json')

@app.route('/icons/<path:filename>')
def icons(filename):
    return send_from_directory(str(BASE_DIR / 'icons'), filename)

@app.route('/')
def index():
    return send_from_directory(str(BASE_DIR), 'INDEX.HTML')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(str(BASE_DIR), filename)


# ── ARRANQUE ──────────────────────────────────────────────────────────────────
def warm_cache():
    print("A pré-carregar cache...")
    cache_set('prices',    fetch_prices())
    cache_set('news',      fetch_news())
    cache_set('feargreed', fetch_feargreed())
    print("Cache pronta.")

db_init()
threading.Thread(target=warm_cache, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"Bitcoin Intelligence · porta {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
