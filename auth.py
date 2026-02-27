import base64
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


class KalshiAuth:
    def __init__(self, api_key, key_path):
        self.api_key = api_key
        with open(key_path, "rb") as f:
            self._priv = serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, msg):
        sig = self._priv.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def rest_headers(self, method, path):
        ts = str(int(time.time() * 1000))
        sig = self._sign(ts + method.upper() + path)
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    def ws_headers(self):
        ts = str(int(time.time() * 1000))
        sig = self._sign(ts + "GET" + "/trade-api/ws/v2")
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }