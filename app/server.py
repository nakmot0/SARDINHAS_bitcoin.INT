import os
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request
import requests

app = Flask(__name__, static_folder='.', static_url_path='')

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "data.json"
HTML_FILE = BASE_DIR / "INDEX.HTML"
MODEL = "mistral:7b-instruct-q4_K_M"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

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

# ── ROTA: Status da Configuração ───────────────────────────────────────────────
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