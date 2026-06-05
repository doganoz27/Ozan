"""
TITAN PRIME — Configuration
"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Finnhub
    FINNHUB_API_KEY: str = "d8ce4jpr01qidic7ibt0d8ce4jpr01qidic7ibtg"
    FINNHUB_BASE_URL: str = "https://finnhub.io/api/v1"

    # Telegram
    TELEGRAM_TOKEN: str = "7731993816:AAHZ1gRt7xxolBzEA9ptAlGv48igZfIuGL0"
    TELEGRAM_CHAT_ID: str = "8237226783"

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://titan:titan_secure_2024@localhost/titan_prime"

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # Account
    ACCOUNT_BALANCE: float = 50.0
    RISK_PCT: float = 0.01
    MAX_RISK_PCT: float = 0.02
    MAX_DAILY_DD: float = 0.03
    MAX_WEEKLY_DD: float = 0.05

    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost", "http://frontend:3000"]

    # Chart paths
    CHART_DIR: str = "/tmp/charts"

    class Config:
        env_file = ".env"
        extra = "allow"


settings = Settings()

# ── Symbol Lists ──────────────────────────────────────────────────────────────
EQ_SYMBOLS = ["NVDA", "AAPL", "SPY", "QQQ", "MSFT", "TSLA", "AMZN", "META", "GOOGL", "JPM"]

FOREX = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY"]

METALS = ["XAUUSD", "XAGUSD"]
OIL = ["USOIL", "BRENT"]

ALL_SYMBOLS = EQ_SYMBOLS + FOREX + METALS + OIL

# Yahoo Finance ticker map
YF_SYMBOLS = {
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "JPY=X",
    "USDCHF": "CHF=X",
    "AUDUSD": "AUDUSD=X",
    "USDCAD": "CAD=X",
    "NZDUSD": "NZDUSD=X",
    "EURGBP": "EURGBP=X",
    "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X",
    "XAUUSD": "GC=F",
    "XAGUSD": "SI=F",
    "USOIL": "CL=F",
    "BRENT": "BZ=F",
}

# Trade212 CFD Leverage
LEVERAGE = {
    "forex": 30,
    "gold": 20,
    "silver": 20,
    "indices": 20,
    "oil": 10,
    "stocks": 5,
    "crypto": 2,
}


def get_leverage(sym: str) -> int:
    if sym in ["XAUUSD"]:
        return LEVERAGE["gold"]
    if sym in ["XAGUSD"]:
        return LEVERAGE["silver"]
    if sym in ["USOIL", "BRENT"]:
        return LEVERAGE["oil"]
    if sym in EQ_SYMBOLS:
        return LEVERAGE["stocks"]
    return LEVERAGE["forex"]


def get_asset_class(sym: str) -> str:
    if sym in ["XAUUSD"]:
        return "GOLD"
    if sym in ["XAGUSD"]:
        return "SILVER"
    if sym in ["USOIL", "BRENT"]:
        return "OIL"
    if sym in EQ_SYMBOLS:
        return "STOCKS"
    return "FOREX"
