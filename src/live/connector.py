import json
import websocket
import threading
import time
import logging
from collections import deque
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class HyperliquidConnector:
    WS_URL = "wss://api.hyperliquid.xyz/ws"

    def __init__(self, coin: str = "SOL"):
        self.coin = coin
        self.ws = None
        self.latest_book: Optional[Dict] = None
        self.lock = threading.Lock()
        self.running = False
        self.thread = None

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            channel = data.get("channel")
            
            if channel == "l2Book":
                # Data format: {channel: 'l2Book', data: {coin: 'SOL', time: 123, levels: [[...], [...]]}}
                content = data.get("data", {})
                if content.get("coin") == self.coin:
                    with self.lock:
                        self.latest_book = content
        except Exception as e:
            logger.error(f"WS Error: {e}")

    def on_error(self, ws, error):
        logger.error(f"WS Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info("WS Connection Closed")

    def on_open(self, ws):
        logger.info("WS Connection Opened")
        # Subscribe
        msg = {
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": self.coin
            }
        }
        ws.send(json.dumps(msg))

    def start(self):
        self.running = True
        # websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        if self.thread:
            self.thread.join(timeout=1)

    def get_latest_book(self):
        with self.lock:
            return self.latest_book
