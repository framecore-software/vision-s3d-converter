from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from orchestrator.config import settings
from orchestrator.queue.redis_client import get_client, get_last_progress

logger = logging.getLogger(__name__)

app = FastAPI(title="vs3d-progress-sse", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins.split(",") if hasattr(settings, "allowed_origins") else ["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> JSONResponse:
    try:
        client = get_client()
        await client.ping()
        return JSONResponse({"status": "ok"})
    except Exception:
        return JSONResponse({"status": "error"}, status_code=503)


@app.get("/progress/{task_id}")
async def progress_stream(task_id: str, request: Request) -> StreamingResponse:
    """
    Server-Sent Events stream para el progreso de una tarea.

    El cliente se suscribe a este endpoint y recibe eventos en tiempo real.
    Si el cliente se reconecta, recibe el último estado conocido inmediatamente.

    Uso desde JavaScript:
        const source = new EventSource(`/progress/${taskId}`);
        source.onmessage = (e) => console.log(JSON.parse(e.data));
    """
    return StreamingResponse(
        _sse_generator(task_id, request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # Deshabilitar buffering en Nginx
            "Connection": "keep-alive",
        },
    )


async def _sse_generator(task_id: str, request: Request) -> AsyncGenerator[str, None]:
    # Enviar último estado conocido inmediatamente al conectar (reconexión)
    last = await get_last_progress(task_id)
    if last:
        yield f"data: {last}\n\n"

    client = get_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(f"task:{task_id}:progress")

    try:
        while True:
            # Verificar si el cliente se desconectó
            if await request.is_disconnected():
                break

            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                data = message["data"]
                yield f"data: {data}\n\n"

                # Si la tarea terminó (completed o failed), cerrar el stream
                try:
                    payload = json.loads(data)
                    if payload.get("status") in ("completed", "failed"):
                        break
                except json.JSONDecodeError:
                    pass

            await asyncio.sleep(0.1)
    finally:
        await pubsub.unsubscribe(f"task:{task_id}:progress")
        await pubsub.aclose()


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("SSE_PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
