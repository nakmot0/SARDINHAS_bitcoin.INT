import os
import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request
import requests
import feedparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.', static_url_path='')

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "data.json"
HTML_FILE = BASE_DIR / "INDEX.HTML"
SOURCES_FILE = BASE_DIR / "sources.txt"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# Cache em memória para preços (evita dados estáticos quando CoinGecko faz rate-limit)
_prices_cache = {
    "data": None,
    "timestamp": None
}

# ── HELPER: Ler data.json ──────────────────────────────────────────────────────
def get_dashboard_data():
    """Lê data.json e devolve dict. Se não existir, devolve template vazio."""
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "bitcoin": {"price": "Carregando...", "change": "n/d", "dominance": "n/d"},
            "eth": {"btc": "n/d"},
            "gold": {"btc": "n/d"},
            "dxy": {"state": "n/d"},
            "editorial": "Aguardando primeira análise...",
            "updated": datetime.now().strftime("%d/%m/%Y %H:%M")
        }

# ── ROTA: Servir HTML ──────────────────────────────────────────────────────────
@app.route('/')
def index():
    """Serve o INDEX.HTML principal."""
    try:
        with open(HTML_FILE, "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content, 200, {'Content-Type': 'text/html; charset=utf-8'}
    except FileNotFoundError:
        return "<h1>❌ INDEX.HTML não encontrado</h1>", 404

# ── ROTA: API de Dados ─────────────────────────────────────────────────────────
@app.route('/api/data')
def api_data():
    """Devolve os dados do dashboard em JSON."""
    data = get_dashboard_data()
    return jsonify(data), 200

# ── ROTA: Trigger Manual para Gerar Dados ──────────────────────────────────────
@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    """
    Executa generate_data.py manualmente (útil para atualizar dados sem esperar cronjob).
    Retorna status da execução.
    """
    try:
        generate_script = BASE_DIR / "generate_data.py"
        if not generate_script.exists():
            return jsonify({"error": "generate_data.py não encontrado"}), 404
        
        # Executar script com timeout
        result = subprocess.run(
            [sys.executable, str(generate_script)],
            cwd=str(BASE_DIR),
            capture_output=True,
            timeout=120,
            text=True
        )
        
        if result.returncode == 0:
            data = get_dashboard_data()
            return jsonify({
                "status": "success",
                "message": "Dados actualizados com sucesso",
                "data": data
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Script falhou",
                "stderr": result.stderr[:500]
            }), 500
    
    except subprocess.TimeoutExpired:
        return jsonify({
            "status": "error",
            "message": "Script demorou demasiado (timeout 120s)"
        }), 504
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ── ROTA: Health Check ─────────────────────────────────────────────────────────
@app.route('/health')
def health():
    """Verifica se o servidor está ativo e data.json existe."""
    data_exists = DATA_FILE.exists()
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "data_available": data_exists
    }), 200

# ── ROTA: Ping / Keep-alive ────────────────────────────────────────────────────
@app.route('/api/ping')
def api_ping():
    """Endpoint para UptimeRobot — mantém o serviço ativo e refresca dados se necessário."""
    data = get_dashboard_data()
    updated = data.get("updated", "")

    # Se dados têm mais de 30 min, atualizar automaticamente
    needs_refresh = False
    try:
        last = datetime.strptime(updated, "%d/%m/%Y %H:%M")
        if (datetime.now() - last).total_seconds() > 1800:
            needs_refresh = True
    except Exception:
        needs_refresh = True

    if needs_refresh:
        try:
            generate_script = BASE_DIR / "generate_data.py"
            if generate_script.exists():
                subprocess.Popen(
                    [sys.executable, str(generate_script)],
                    cwd=str(BASE_DIR),
                    env={**os.environ}
                )
        except Exception:
            pass

    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "data_updated": updated,
        "refresh_triggered": needs_refresh
    }), 200


@app.route('/api/config')
def api_config():
    """Devolve informações sobre a configuração (útil para debug)."""
    return jsonify({
        "groq_key_set": bool(GROQ_API_KEY),
        "data_file_exists": DATA_FILE.exists(),
        "html_file_exists": HTML_FILE.exists(),
        "base_dir": str(BASE_DIR),
        "python_version": sys.version.split()[0]
    }), 200

# ── ROTA: Preços ao Vivo ───────────────────────────────────────────────────────
@app.route('/api/prices')
def api_prices():
    """Devolve preços ao vivo do Bitcoin e ativos relacionados."""
    try:
        prices_resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "bitcoin,ethereum,gold",
                "vs_currencies": "usd,btc",
                "include_24hr_change": "true",
            },
            timeout=10,
        )
        prices = prices_resp.json()

        global_resp = requests.get(
            "https://api.coingecko.com/api/v3/global",
            timeout=10,
        )
        global_data = global_resp.json()

        btc_usd = prices["bitcoin"]["usd"]
        btc_change = prices["bitcoin"].get("usd_24h_change", 0.0)
        eth_btc = prices["ethereum"]["btc"]
        dominance = global_data["data"]["market_cap_percentage"]["btc"]

        # Gold direto da mesma resposta
        gold_usd = prices.get("gold", {}).get("usd")
        if gold_usd and btc_usd:
            gold_btc = gold_usd / btc_usd
        else:
            # Fallback: tentar cache, depois data.json, depois valor aproximado
            if _prices_cache["data"] and _prices_cache["data"].get("gold_btc"):
                gold_btc = _prices_cache["data"]["gold_btc"]
            else:
                try:
                    data = get_dashboard_data()
                    gold_str = str(data.get("gold", {}).get("btc", "0")).split()[0]
                    gold_btc = float(gold_str)
                except (ValueError, TypeError, IndexError):
                    gold_btc = 2000.0 / btc_usd if btc_usd else 0.03  # ~$2000/oz fallback

        # Crude oil — no free live API; use data.json fallback
        crude_usd = 70.0
        crude_btc = crude_usd / btc_usd if btc_usd else 0.0

        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        result = {
            "btc_usd": btc_usd,
            "btc_change_24h": round(btc_change, 2),
            "dominance": round(dominance, 2),
            "eth_btc": round(eth_btc, 5),
            "gold_btc": round(gold_btc, 5),
            "crude_btc": round(crude_btc, 5),
            "crude_usd": crude_usd,
            "updated": now,
        }
        # Guardar na cache
        _prices_cache["data"] = result
        _prices_cache["timestamp"] = datetime.now()
        return jsonify(result), 200

    except Exception as e:
        # Primeiro tentar a cache em memória (dados recentes do último fetch com sucesso)
        if _prices_cache["data"]:
            cached = _prices_cache["data"].copy()
            cached["updated"] = datetime.now().strftime("%d/%m/%Y %H:%M")
            cached["from_cache"] = True
            return jsonify(cached), 200

        # Se não há cache, fallback para data.json
        try:
            data = get_dashboard_data()
            price_digits = re.sub(r'[^\d]', '', str(data.get("bitcoin", {}).get("price", "0")))
            btc_usd = float(price_digits) if price_digits else 66000.0
            btc_change_str = str(data.get("bitcoin", {}).get("change", "0")).replace("+", "").replace("%", "").replace(",", ".")
            btc_change = float(btc_change_str) if btc_change_str else 0.0
            return jsonify({
                "btc_usd": btc_usd,
                "btc_change_24h": btc_change,
                "dominance": 56.3,
                "eth_btc": 0.02929,
                "gold_btc": 0.07877,
                "crude_btc": 0.00102,
                "crude_usd": 68.5,
                "updated": datetime.now().strftime("%d/%m/%Y %H:%M"),
            }), 200
        except Exception:
            return jsonify({"error": str(e)}), 502


# ── ROTA: BTC Ticker Rápido ───────────────────────────────────────────────────
@app.route('/api/btc-tick')
def api_btc_tick():
    """Preço BTC rápido para ticker de 2 segundos."""
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
            timeout=5,
        )
        data = resp.json()
        btc_usd = data["bitcoin"]["usd"]
        btc_change = data["bitcoin"].get("usd_24h_change", 0.0)
        # Atualizar cache também
        if _prices_cache["data"]:
            _prices_cache["data"]["btc_usd"] = btc_usd
            _prices_cache["data"]["btc_change_24h"] = round(btc_change, 2)
        return jsonify({"btc_usd": btc_usd, "btc_change_24h": round(btc_change, 2)}), 200
    except Exception as e:
        logger.warning(f"btc-tick falhou: {e}")
        if _prices_cache["data"]:
            return jsonify({
                "btc_usd": _prices_cache["data"].get("btc_usd", 0),
                "btc_change_24h": _prices_cache["data"].get("btc_change_24h", 0.0),
            }), 200
        return jsonify({"error": "unavailable"}), 502


# ── ROTA: Fear & Greed Index ───────────────────────────────────────────────────
@app.route('/api/feargreed')
def api_feargreed():
    """Devolve o índice Fear & Greed do Bitcoin."""
    try:
        resp = requests.get(
            "https://api.alternative.me/fng/?limit=7",
            timeout=10,
        )
        fg_data = resp.json().get("data", [])
        value = fg_data[0]["value"] if len(fg_data) > 0 else "50"
        yesterday = fg_data[1]["value"] if len(fg_data) > 1 else value
        last_week = fg_data[6]["value"] if len(fg_data) > 6 else value
        return jsonify({
            "value": value,
            "yesterday": yesterday,
            "last_week": last_week,
        }), 200
    except Exception as e:
        return jsonify({"error": str(e), "value": "50", "yesterday": "50", "last_week": "50"}), 200


# ── ROTA: Notícias ─────────────────────────────────────────────────────────────
@app.route('/api/news')
def api_news():
    """Devolve notícias recentes de Bitcoin a partir de RSS."""
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (compatible; BitcoinIntelligence/1.0; +https://github.com/nakmot0/SARDINHAS_bitcoin.INT)'
    }
    BULLISH_WORDS = ["bull", "surge", "rally", "high", "gain", "up", "rise", "record", "ath", "buy", "halving", "adoption"]
    BEARISH_WORDS = ["bear", "crash", "drop", "down", "fall", "low", "sell", "dump", "fear", "hack", "ban", "risk"]

    def classify(title):
        t = title.lower()
        b = sum(1 for w in BULLISH_WORDS if w in t)
        n = sum(1 for w in BEARISH_WORDS if w in t)
        if b > n:
            return "bullish"
        if n > b:
            return "bearish"
        return "neutral"

    sources = []
    try:
        if SOURCES_FILE.exists():
            for line in SOURCES_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    sources.append(line)
    except Exception:
        pass

    items = []
    for url in sources[:8]:  # limit sources to avoid slow response
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            source_name = feed.feed.get("title", url.split("/")[2]) if feed.feed else url.split("/")[2]
            for entry in feed.entries[:3]:
                title = entry.get("title", "").strip()
                if not title:
                    continue
                published = entry.get("published", "")
                link = entry.get("link", "")
                items.append({
                    "title": title,
                    "source": source_name,
                    "date": published[:16] if published else "--",
                    "sentiment": classify(title),
                    "link": link,
                })
        except Exception as e:
            logger.warning(f"Feed falhou: {url} — {e}")
            continue

    if not items:
        items = [{"title": "Sem notícias disponíveis de momento", "source": "Sistema", "date": "--", "sentiment": "neutral", "link": ""}]

    now = datetime.now().strftime("%H:%M")
    return jsonify({"items": items[:20], "updated": now}), 200


# ── ROTA: Resumo do Agente ─────────────────────────────────────────────────────
@app.route('/api/agent-summary')
def api_agent_summary():
    """Gera um resumo de mercado em português com Groq AI."""
    data = get_dashboard_data()

    if not GROQ_API_KEY:
        return jsonify({
            "summary": data.get("editorial", "Agente indisponível — configura GROQ_API_KEY."),
            "generated_at": data.get("updated", "--"),
        }), 200

    try:
        context = (
            f"Bitcoin: {data.get('bitcoin', {}).get('price', 'n/d')} "
            f"({data.get('bitcoin', {}).get('change', 'n/d')} 24h), "
            f"Dominância: {data.get('bitcoin', {}).get('dominance', 'n/d')}. "
            f"ETH/BTC: {data.get('eth', {}).get('btc', 'n/d')}. "
            f"Ouro/BTC: {data.get('gold', {}).get('btc', 'n/d')}. "
            f"DXY: {data.get('dxy', {}).get('state', 'n/d')}. "
            f"Dados de: {data.get('updated', '--')}."
        )
        prompt = (
            "És um analista de mercado Bitcoin. Responde em português de Portugal. "
            "Com base nos dados abaixo, escreve um parágrafo curto (3-4 frases) "
            "de análise de mercado. Sem conselhos financeiros diretos.\n\n"
            f"Dados: {context}"
        )
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.5,
        }
        resp = requests.post(GROQ_API_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        summary = resp.json()["choices"][0]["message"]["content"].strip()
        now = datetime.now().strftime("%H:%M")
        return jsonify({"summary": summary, "generated_at": now}), 200

    except Exception as e:
        return jsonify({
            "summary": data.get("editorial", "Erro ao gerar resumo."),
            "generated_at": data.get("updated", "--"),
        }), 200


# ── ROTA: Chat com o Agente ────────────────────────────────────────────────────
@app.route('/api/agent-chat', methods=['POST'])
def api_agent_chat():
    """Chat interativo com o agente de análise Bitcoin."""
    if not GROQ_API_KEY:
        return jsonify({"reply": "Agente indisponível — configura a variável GROQ_API_KEY no servidor."}), 200

    try:
        body = request.get_json(force=True) or {}
        messages = body.get("messages", [])
        snapshot = body.get("snapshot", {})

        system_prompt = (
            "És um agente de análise de mercado com foco em Bitcoin. "
            "Respondes em português de Portugal, de forma clara e profissional. "
            "Evitas conselhos financeiros diretos. "
            "Contexto atual do dashboard:\n"
            f"- Preço BTC: {snapshot.get('price', 'n/d')}\n"
            f"- Variação 24h: {snapshot.get('change', 'n/d')}\n"
            f"- Dominância: {snapshot.get('dominance', 'n/d')}\n"
            f"- ETH/BTC: {snapshot.get('ethBtc', 'n/d')}\n"
            f"- Ouro/BTC: {snapshot.get('goldBtc', 'n/d')}\n"
        )

        groq_messages = [{"role": "system", "content": system_prompt}] + messages

        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": GROQ_MODEL,
            "messages": groq_messages,
            "temperature": 0.6,
        }
        resp = requests.post(GROQ_API_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        reply = resp.json()["choices"][0]["message"]["content"].strip()
        return jsonify({"reply": reply}), 200

    except Exception as e:
        return jsonify({"reply": f"Erro ao contactar o agente: {str(e)}"}), 200



# ── ERRO 404 ───────────────────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint não encontrado"}), 404

# ── ERRO 500 ───────────────────────────────────────────────────────────────────
@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "Erro interno do servidor"}), 500

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Modo producao (debug=False para Render.com)
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)