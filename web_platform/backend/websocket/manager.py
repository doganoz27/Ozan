"""
TITAN PRIME — WebSocket Connection Manager
Handles all connected clients and broadcasts real-time updates.
"""
import asyncio
import json
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState


class ConnectionManager:
    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.append(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            if websocket in self._connections:
                self._connections.remove(websocket)

    async def broadcast(self, message: dict) -> None:
        """Broadcast a message to all connected clients."""
        if not self._connections:
            return
        text = json.dumps(message)
        dead: list[WebSocket] = []
        async with self._lock:
            targets = list(self._connections)
        for ws in targets:
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_text(text)
                else:
                    dead.append(ws)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    if ws in self._connections:
                        self._connections.remove(ws)

    async def send_personal(self, websocket: WebSocket, message: dict) -> None:
        """Send a message to a specific client."""
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(json.dumps(message))
        except Exception:
            await self.disconnect(websocket)

    @property
    def connection_count(self) -> int:
        return len(self._connections)


# Singleton instance
manager = ConnectionManager()


async def broadcast_price_update(prices: dict) -> None:
    """Broadcast price updates to all clients."""
    await manager.broadcast({
        "type": "price_update",
        "data": prices,
        "ts": __import__("time").time(),
    })


async def broadcast_new_signal(signal: dict) -> None:
    """Broadcast a new signal to all clients."""
    await manager.broadcast({
        "type": "new_signal",
        "data": signal,
        "ts": __import__("time").time(),
    })


async def broadcast_trade_update(trade: dict) -> None:
    """Broadcast a trade update to all clients."""
    await manager.broadcast({
        "type": "trade_update",
        "data": trade,
        "ts": __import__("time").time(),
    })


async def broadcast_news(article: dict) -> None:
    """Broadcast a news item to all clients."""
    await manager.broadcast({
        "type": "news",
        "data": article,
        "ts": __import__("time").time(),
    })
