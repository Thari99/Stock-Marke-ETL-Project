import time
import json
from kafka import KafkaProducer
import requests

API_KEY = "d7ch3upr01qv03esbangd7ch3upr01qv03esbao0"  
BROKER_URL = "https://finnhub.io/api/v1/quote"
SYMBOL = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"} 

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_quote(symbol):
    url = f"{BROKER_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    
    except requests.RequestException as e:
        print(f"Error fetching quote for {symbol}: {e}")
        return {"symbol": symbol, "error": str(e)}
    
while True:
    for symbol in SYMBOL:
        quote = fetch_quote(symbol)
        if quote:
            print(f"producer:{quote}")
            producer.send("stock-quotes", value=quote)
        
    time.sleep(6)