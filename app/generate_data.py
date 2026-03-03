"""
generate_data.py — Gerador de dados para Bitcoin Intelligence
Busca dados reais via CoinGecko (gratuito, sem API key) e injeta no prompt
para que o modelo produza análise contextualizada.
"""

import json
import subprocess
import sys
import requests
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
from pathlib import Path

BASE_DIR      = Path(__file__).parent
MODEL         = "mistral:7b-instruct-q4_K_M"
PROMPT_FILE   = BASE_DIR / "prompt.txt"
OUTPUT_FILE   = BASE_DIR / "data.json"
MODEL_TIMEOUT = 600  # 10 minutos — 32B pode demorar na primeira carga para VRAM

# ── 1. FETCH DADOS REAIS ─────────────────────────────────────────────────────
def get_market_data():
    """Busca dados reais de mercado. Devolve dict ou None se falhar."""
    print("📡 A buscar dados de mercado...")
    result = {}

    # BTC price — Coinbase
    try:
        r = requests.get('https://api.coinbase.com/v2/prices/BTC-USD/spot', timeout=8)
        r.raise_for_status()
        btc_price = float(r.json()['data']['amount'])
        result['btc_price'] = f"${btc_price:,.0f}".replace(",", ".")
        print(f"✅ Coinbase: {result['btc_price']}")
    except Exception as e:
        print(f"⚠️  Coinbase falhou: {e}")

    # CoinGecko — variação, ETH/BTC, ouro
    try:
        r = requests.get(
            'https://api.coingecko.com/api/v3/simple/price'
            '?ids=bitcoin,ethereum,tether-gold&vs_currencies=usd,btc&include_24hr_change=true',
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        btc = data.get('bitcoin', {})
        eth = data.get('ethereum', {})
        gold = data.get('tether-gold', {})

        if 'btc_price' not in result:
            result['btc_price'] = f"${btc.get('usd', 0):,.0f}".replace(",", ".")

        change = btc.get('usd_24h_change', 0)
        result['btc_change'] = f"{change:+.2f}%".replace(".", ",")
        result['eth_in_btc'] = f"{eth.get('btc', 0):.5f} BTC"
        if gold:
            result['gold_in_btc'] = f"{gold.get('btc', 0):.5f} BTC"
        print(f"✅ CoinGecko: change={result['btc_change']} ETH={result['eth_in_btc']}")
    except Exception as e:
        print(f"⚠️  CoinGecko falhou: {e}")

    # Dominância — tenta CoinGecko global, fallback para mercado de caps
    try:
        import time
        time.sleep(1.5)  # evitar rate limit após chamada anterior ao CoinGecko
        r = requests.get('https://api.coingecko.com/api/v3/global', timeout=10)
        r.raise_for_status()
        dom = r.json()['data']['market_cap_percentage']['btc']
        result['dominance'] = f"{dom:.1f}%"
        print(f"✅ Dominância: {result['dominance']}")
    except Exception:
        # Fallback: CoinCapo API (sem rate limit agressivo)
        try:
            r = requests.get('https://api.coincap.io/v2/assets/bitcoin', timeout=8)
            r.raise_for_status()
            d = r.json().get('data', {})
            # CoinCap não dá dominância direta, mas dá market cap
            # Vamos estimar a partir do total market via segunda chamada
            r2 = requests.get('https://api.coincap.io/v2/assets?limit=1', timeout=8)
            # Se falhar usa valor do data.json anterior se existir
            raise Exception("usar fallback json")
        except Exception:
            # Tentar ler do data.json anterior para não perder o valor
            try:
                with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                    old = json.load(f)
                result['dominance'] = old.get('bitcoin', {}).get('dominance', 'n/d')
                print(f"⚠️  Dominância: usando valor anterior → {result['dominance']}")
            except Exception:
                result['dominance'] = 'n/d'
                print("⚠️  Dominância indisponível")

    # DXY via stooq — parse robusto (fim de semana devolve "N/D")
    try:
        r = requests.get('https://stooq.com/q/l/?s=dxy&f=sd2t2ohlcv&h&e=csv', timeout=8)
        r.raise_for_status()
        lines = [l.strip() for l in r.text.strip().split('\n') if l.strip()]
        dxy_val = None
        # procura a primeira linha com valor numérico válido (ignora "N/D")
        for line in reversed(lines[1:]):  # ignora header
            cols = line.split(',')
            for col in reversed(cols):
                try:
                    v = float(col)
                    if 80 < v < 130:  # range realista do DXY
                        dxy_val = v
                        break
                except ValueError:
                    continue
            if dxy_val:
                break
        if dxy_val:
            if dxy_val >= 103:
                result['dxy_state'] = 'Forte'
            elif dxy_val <= 100:
                result['dxy_state'] = 'Fraco'
            else:
                result['dxy_state'] = 'Estável'
            result['dxy_val'] = dxy_val
            print(f"✅ DXY: {dxy_val:.2f} → {result['dxy_state']}")
        else:
            print("⚠️  DXY: valores N/D (mercado fechado?)")
    except Exception as e:
        print(f"⚠️  DXY falhou: {e}")

    return result if result else None


# ── 2. LER PROMPT ────────────────────────────────────────────────────────────
with open(PROMPT_FILE, "r", encoding="utf-8") as f:
    base_prompt = f.read()

# ── 3. INJETAR DADOS REAIS NO PROMPT ─────────────────────────────────────────
market = get_market_data()

if market:
    dxy_line = f"- DXY (índice do dólar): {market.get('dxy_val', 'n/d')} → Estado: {market.get('dxy_state', 'n/d')}" if 'dxy_val' in market else ""
    data_block = f"""
DADOS REAIS DE MERCADO (usa estes valores exatos no JSON):
- Bitcoin (USD): {market.get('btc_price', 'n/d')}
- Variação BTC 24h: {market.get('btc_change', 'n/d')}
- ETH em BTC: {market.get('eth_in_btc', 'n/d')}
- Ouro (XAUT) em BTC: {market.get('gold_in_btc', 'dados indisponíveis')}
- Dominância BTC: {market.get('dominance', 'n/d')}
{dxy_line}

"""
    prompt = data_block + base_prompt
    btc_str = market.get('btc_price', 'n/d')
    dom_str = market.get('dominance', 'n/d')
    print(f"✅ Dados reais injetados: BTC={btc_str} | Dom={dom_str}")
else:
    prompt = base_prompt
    print("⚠️  A usar prompt base sem dados reais.")

# ── 4. CORRER O MODELO ───────────────────────────────────────────────────────
print(f"🤖 A correr modelo {MODEL}...")
print(f"   (pode demorar até {MODEL_TIMEOUT//60} min na primeira execução — a carregar para VRAM)")

try:
    result = subprocess.run(
        ["ollama", "run", MODEL],
        input=prompt.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=MODEL_TIMEOUT
    )
except subprocess.TimeoutExpired:
    print(f"❌ O modelo excedeu {MODEL_TIMEOUT}s. Tenta aumentar MODEL_TIMEOUT ou usar um modelo mais pequeno.")
    sys.exit(1)

raw_output = result.stdout.decode("utf-8", errors="ignore").strip()

if not raw_output:
    print("❌ O modelo não devolveu qualquer output")
    print("STDERR:", result.stderr.decode("utf-8", errors="ignore")[:500])
    sys.exit(1)

# ── 5. EXTRAIR JSON (tolerante a texto extra do modelo) ───────────────────────
def extract_json(text):
    """Tenta extrair JSON mesmo que o modelo acrescente texto à volta."""
    # Tentativa 1: output limpo
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Tentativa 2: encontrar { ... } maior
    start = text.find('{')
    end   = text.rfind('}')
    if start != -1 and end != -1:
        try:
            return json.loads(text[start:end+1])
        except json.JSONDecodeError:
            pass
    return None

data = extract_json(raw_output)
if not data:
    print("❌ Output inválido (não é JSON):")
    print(raw_output[:1000])
    sys.exit(1)

# ── 6. VALIDAÇÃO ──────────────────────────────────────────────────────────────
required = ["bitcoin", "gold", "eth", "dxy", "editorial"]
for key in required:
    if key not in data:
        print(f"❌ Chave obrigatória em falta: {key}")
        sys.exit(1)

# ── 7. SOBRESCREVER COM DADOS REAIS (garante precisão) ────────────────────────
if market:
    if market.get('btc_price'):   data["bitcoin"]["price"]     = market["btc_price"]
    if market.get('btc_change'):  data["bitcoin"]["change"]    = market["btc_change"]
    if market.get('dominance'):   data["bitcoin"]["dominance"] = market["dominance"]
    if market.get('eth_in_btc'):  data["eth"]["btc"]           = market["eth_in_btc"]
    if market.get('gold_in_btc'): data["gold"]["btc"]          = market["gold_in_btc"]
    if market.get('dxy_state'):   data["dxy"]["state"]         = market["dxy_state"]

# ── 8. TIMESTAMP ──────────────────────────────────────────────────────────────
data["updated"] = datetime.now().strftime("%d/%m/%Y %H:%M")

# ── 9. GUARDAR ────────────────────────────────────────────────────────────────
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"✅ {OUTPUT_FILE} actualizado com sucesso — {data['updated']}")
