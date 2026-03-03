# Updated version of server.py

# Corrected and consolidated version that removes duplicate code and fixes the gold API issue.

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/get_price', methods=['GET'])
def get_price():
    try:
        response = requests.get('https://api.example.com/gold_price')  # Example placeholder
        response.raise_for_status()
        data = response.json()
        price = data['price']  # Adjust as per actual API response structure
        return jsonify({'price': price}), 200
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)