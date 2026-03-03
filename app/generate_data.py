import os
import json
import requests
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "data.json"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"


def fetch_market_data():
    """Fetch live market data from CoinGecko free API."""
    prices = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": "bitcoin,ethereum,gold",
            "vs_currencies": "usd,btc",
            "include_24hr_change": "true",
        },
        timeout=15,
    ).json()

    global_data = requests.get(
        "https://api.coingecko.com/api/v3/global",
        timeout=15,
    ).json()

    btc_usd = prices["bitcoin"]["usd"]
    btc_change = prices["bitcoin"].get("usd_24h_change", 0.0)
    eth_btc = prices["ethereum"]["btc"]
    dominance = global_data["data"]["market_cap_percentage"]["btc"]

    # Gold: ~1 troy oz in USD / BTC price
    gold_usd_per_oz = 1900.0  # fallback
    try:
        gold_resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "gold", "vs_currencies": "usd"},
            timeout=10,
        ).json()
        gold_usd_per_oz = gold_resp.get("gold", {}).get("usd", gold_usd_per_oz)
    except Exception:
        pass
    gold_btc = gold_usd_per_oz / btc_usd if btc_usd else 0.0

    # Crude oil fallback (no free live API without key)
    crude_usd = 70.0
    crude_btc = crude_usd / btc_usd if btc_usd else 0.0

    return {
        "btc_usd": btc_usd,
        "btc_change_24h": btc_change,
        "dominance": dominance,
        "eth_btc": eth_btc,
        "gold_btc": gold_btc,
        "crude_btc": crude_btc,
        "crude_usd": crude_usd,
    }


def call_groq(api_key, prompt_text):
    """Call Groq API and return the response text."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": 0.4,
    }
    resp = requests.post(GROQ_API_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def format_change(change_pct):
    """Format a percentage change value with sign and Portuguese decimal comma."""
    sign = "+" if change_pct >= 0 else ""
    return f"{sign}{change_pct:.2f}%".replace(".", ",")


def build_data_json(market, groq_text):
    """Parse Groq JSON response and merge with market data."""
    try:
        parsed = json.loads(groq_text)
    except json.JSONDecodeError:
        # Try to extract JSON block from text
        start = groq_text.find("{")
        end = groq_text.rfind("}") + 1
        parsed = json.loads(groq_text[start:end]) if start != -1 else {}

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    parsed["updated"] = now

    # Ensure required fields exist with live market data as fallback
    btc = parsed.setdefault("bitcoin", {})
    btc.setdefault("price", f"${market['btc_usd']:,.0f}")
    btc.setdefault("change", format_change(market["btc_change_24h"]))
    btc.setdefault("dominance", f"{market['dominance']:.1f}%")
    btc.setdefault("dominance_trend", "n/d")
    btc.setdefault("summary", "n/d")

    parsed.setdefault("gold", {"btc": f"{market['gold_btc']:.5f} BTC", "trend": "n/d"})
    parsed.setdefault("eth", {"btc": f"{market['eth_btc']:.5f} BTC", "trend": "n/d"})
    parsed.setdefault("dxy", {"state": "n/d", "summary": "n/d"})
    parsed.setdefault("editorial", "Análise indisponível.")

    return parsed


if __name__ == "__main__":
    print("A obter dados de mercado...")
    try:
        market = fetch_market_data()
        print(f"BTC: ${market['btc_usd']:,.0f} ({market['btc_change_24h']:+.2f}%)")
    except Exception as e:
        print(f"Erro ao obter dados de mercado: {e}")
        raise

    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        print("AVISO: GROQ_API_KEY não definida. A usar dados básicos sem análise AI.")
        data = {
            "updated": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "bitcoin": {
                "price": f"${market['btc_usd']:,.0f}",
                "change": format_change(market["btc_change_24h"]),
                "dominance": f"{market['dominance']:.1f}%",
                "dominance_trend": "n/d",
                "summary": "n/d",
            },
            "gold": {"btc": f"{market['gold_btc']:.5f} BTC", "trend": "n/d"},
            "eth": {"btc": f"{market['eth_btc']:.5f} BTC", "trend": "n/d"},
            "dxy": {"state": "n/d", "summary": "n/d"},
            "editorial": "Chave Groq não configurada — análise AI indisponível.",
        }
    else:
        try:
            prompt_path = BASE_DIR / "prompt.txt"
            base_prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""
            market_context = (
                f"\nDADOS DE MERCADO ATUAIS:\n"
                f"- Bitcoin: ${market['btc_usd']:,.0f} ({market['btc_change_24h']:+.2f}% 24h)\n"
                f"- Dominância BTC: {market['dominance']:.1f}%\n"
                f"- ETH/BTC: {market['eth_btc']:.5f}\n"
                f"- Ouro/BTC: {market['gold_btc']:.5f}\n"
            )
            print("A chamar Groq API...")
            groq_text = call_groq(api_key, base_prompt + market_context)
            data = build_data_json(market, groq_text)
        except Exception as e:
            print(f"Erro na chamada Groq: {e}")
            data = {
                "updated": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "bitcoin": {
                    "price": f"${market['btc_usd']:,.0f}",
                    "change": format_change(market["btc_change_24h"]),
                    "dominance": f"{market['dominance']:.1f}%",
                    "dominance_trend": "n/d",
                    "summary": "n/d",
                },
                "gold": {"btc": f"{market['gold_btc']:.5f} BTC", "trend": "n/d"},
                "eth": {"btc": f"{market['eth_btc']:.5f} BTC", "trend": "n/d"},
                "dxy": {"state": "n/d", "summary": "n/d"},
                "editorial": f"Erro na análise AI: {e}",
            }

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"data.json guardado em {DATA_FILE}")
