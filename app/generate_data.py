"""
generate_data.py — Gera data.json com análise Groq
Corre manualmente ou via /api/ping (keep-alive automático)
"""
import os
import json
import time
import requests
from datetime import datetime
from pathlib import Path

BASE_DIR     = Path(__file__).parent
DATA_FILE    = BASE_DIR / "data.json"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.1-8b-instant"

H = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}


def stooq_val(ticker, lo, hi):
    """Busca valor do Stooq sem bloqueio em cloud."""
    r = requests.get(f'https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv',
                     headers=H, timeout=12)
    r.raise_for_status()
    for line in reversed([l.strip() for l in r.text.strip().split('\n') if l.strip()][1:]):
        for col in reversed(line.split(',')):
            try:
                v = float(col)
                if lo < v < hi: return v
            except Exception: continue
    return None


def fetch_market_data():
    """Busca dados de mercado com fallbacks robustos (sem CoinGecko em cloud)."""
    result = {
        'btc_usd': None, 'btc_change_24h': 0.0,
        'eth_btc': None, 'gold_btc': None,
        'dominance': None, 'crude_usd': None, 'crude_btc': None,
    }

    # BTC + ETH/BTC — Kraken
    try:
        r = requests.get('https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHXBT',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        for k in ['XXBTZUSD', 'XBTUSD']:
            if k in d: result['btc_usd'] = float(d[k]['c'][0]); break
        for k in ['XETHXXBT', 'ETHXBT']:
            if k in d: result['eth_btc'] = float(d[k]['c'][0]); break
        print(f"BTC: ${result['btc_usd']:,.0f} | ETH/BTC: {result['eth_btc']}")
    except Exception as e:
        print(f"Kraken: {e}")

    # Fallback BTC: Coinbase
    if result['btc_usd'] is None:
        try:
            r = requests.get('https://api.coinbase.com/v2/prices/BTC-USD/spot',
                             headers=H, timeout=8)
            result['btc_usd'] = float(r.json()['data']['amount'])
            print(f"BTC (Coinbase): ${result['btc_usd']:,.0f}")
        except Exception as e:
            print(f"Coinbase: {e}")

    # Variação 24h — Kraken OHLC
    try:
        r = requests.get('https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1440',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next((k for k in d if k != 'last'), None)
        if key and len(d[key]) >= 2:
            o, c = float(d[key][-2][1]), float(d[key][-1][4])
            if o > 0: result['btc_change_24h'] = round((c - o) / o * 100, 3)
    except Exception as e:
        print(f"Kraken OHLC: {e}")

    # Dominância — Coinpaprika
    try:
        r = requests.get('https://api.coinpaprika.com/v1/global', headers=H, timeout=10)
        r.raise_for_status()
        result['dominance'] = r.json().get('bitcoin_dominance_percentage', 55.0)
    except Exception as e:
        print(f"Coinpaprika: {e}")
        result['dominance'] = 55.0

    # Ouro — Kraken XAUUSD
    gold_usd = None
    try:
        r = requests.get('https://api.kraken.com/0/public/Ticker?pair=XAUUSD',
                         headers=H, timeout=10)
        r.raise_for_status(); d = r.json().get('result', {})
        key = next(iter(d), None)
        if key: gold_usd = float(d[key]['c'][0])
        print(f"Ouro (Kraken): ${gold_usd:.0f}")
    except Exception as e:
        print(f"Kraken ouro: {e}")

    if not gold_usd:
        try:
            gold_usd = stooq_val('gc.f', 1500, 5000)
            if gold_usd: print(f"Ouro (Stooq): ${gold_usd:.0f}")
        except Exception as e:
            print(f"Stooq ouro: {e}")

    if gold_usd and result['btc_usd']:
        result['gold_btc'] = round(gold_usd / result['btc_usd'], 6)

    # Petróleo WTI — Stooq CL.F (real, não hardcoded)
    try:
        crude_usd = stooq_val('cl.f', 40, 200)
        if crude_usd:
            result['crude_usd'] = crude_usd
            if result['btc_usd']:
                result['crude_btc'] = round(crude_usd / result['btc_usd'], 6)
            print(f"WTI: ${crude_usd:.1f}")
    except Exception as e:
        print(f"Stooq WTI: {e}")

    if result['btc_usd'] is None:
        raise ValueError("Não foi possível obter preço BTC de nenhuma fonte.")

    return result


def call_groq(api_key, prompt_text):
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type':  'application/json',
    }
    payload = {
        'model':       GROQ_MODEL,
        'messages':    [{'role': 'user', 'content': prompt_text}],
        'temperature': 0.4,
    }
    resp = requests.post(GROQ_API_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content']


def format_change(pct):
    sign = '+' if pct >= 0 else ''
    return f"{sign}{pct:.2f}%".replace('.', ',')


def build_data_json(market, groq_text):
    try:
        parsed = json.loads(groq_text)
    except json.JSONDecodeError:
        s = groq_text.find('{'); e = groq_text.rfind('}') + 1
        parsed = json.loads(groq_text[s:e]) if s != -1 else {}

    parsed['updated'] = datetime.now().strftime('%d/%m/%Y %H:%M')

    btc = parsed.setdefault('bitcoin', {})
    btc.setdefault('price', f"${market['btc_usd']:,.0f}")
    btc.setdefault('change', format_change(market['btc_change_24h']))
    btc.setdefault('dominance', f"{market['dominance']:.1f}%")
    btc.setdefault('dominance_trend', 'n/d')
    btc.setdefault('summary', 'n/d')

    gold_str = f"{market['gold_btc']:.5f} BTC" if market.get('gold_btc') else 'n/d'
    eth_str  = f"{market['eth_btc']:.5f} BTC"  if market.get('eth_btc')  else 'n/d'

    parsed.setdefault('gold', {'btc': gold_str,  'trend': 'n/d'})
    parsed.setdefault('eth',  {'btc': eth_str,   'trend': 'n/d'})
    parsed.setdefault('dxy',  {'state': 'n/d',   'summary': 'n/d'})
    parsed.setdefault('editorial', 'Análise indisponível.')

    return parsed


def build_fallback(market, reason=''):
    gold_str = f"{market['gold_btc']:.5f} BTC" if market.get('gold_btc') else 'n/d'
    eth_str  = f"{market['eth_btc']:.5f} BTC"  if market.get('eth_btc')  else 'n/d'
    return {
        'updated': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'bitcoin': {
            'price':           f"${market['btc_usd']:,.0f}",
            'change':          format_change(market['btc_change_24h']),
            'dominance':       f"{market['dominance']:.1f}%",
            'dominance_trend': 'n/d',
            'summary':         'n/d',
        },
        'gold':     {'btc': gold_str, 'trend': 'n/d'},
        'eth':      {'btc': eth_str,  'trend': 'n/d'},
        'dxy':      {'state': 'n/d',  'summary': 'n/d'},
        'editorial': reason or 'Análise AI indisponível.',
    }


if __name__ == '__main__':
    print('A obter dados de mercado...')
    market = fetch_market_data()
    print(f"BTC: ${market['btc_usd']:,.0f} ({market['btc_change_24h']:+.2f}%)")

    api_key = os.environ.get('GROQ_API_KEY', '')
    if not api_key:
        print('AVISO: GROQ_API_KEY não definida. A guardar dados básicos.')
        data = build_fallback(market, 'Chave Groq não configurada.')
    else:
        try:
            prompt_path = BASE_DIR / 'prompt.txt'
            base_prompt = prompt_path.read_text(encoding='utf-8') if prompt_path.exists() else (
                "És um analista Bitcoin. Responde em português de Portugal. "
                "Gera um JSON com: bitcoin.summary, gold.trend, eth.trend, dxy.state, editorial. "
                "Responde APENAS com JSON válido, sem texto extra."
            )
            context = (
                f"\nDADOS ACTUAIS:\n"
                f"- BTC: ${market['btc_usd']:,.0f} ({market['btc_change_24h']:+.2f}% 24h)\n"
                f"- Dominância: {market['dominance']:.1f}%\n"
                f"- ETH/BTC: {market.get('eth_btc') or 'n/d'}\n"
                f"- Ouro/BTC: {market.get('gold_btc') or 'n/d'}\n"
                f"- WTI: ${market.get('crude_usd') or 'n/d'}\n"
            )
            print('A chamar Groq API...')
            groq_text = call_groq(api_key, base_prompt + context)
            data = build_data_json(market, groq_text)
            print('Groq OK.')
        except Exception as e:
            print(f"Erro Groq: {e}")
            data = build_fallback(market, f"Erro na análise AI: {e}")

    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"data.json guardado — BTC {data['bitcoin']['price']}")
