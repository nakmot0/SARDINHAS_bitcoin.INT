from flask import Flask, jsonify, render_template
import generate_data

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    data = generate_data.get_market_data()  # Assumed function to get Bitcoin market data
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)