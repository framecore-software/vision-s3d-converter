from __future__ import annotations

import json
import logging
import time
from typing import Optional

import redis

from orchestrator.config import settings

logger = logging.getLogger(__name__)

# Cliente síncrono para uso dentro de subprocesos worker
# (los workers corren en subprocess, no en el event loop async del orchestrator)
_client: redis.Redis | None = None


def _get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
    return _client


class ProgressReporter:
    """
    Publica actualizaciones de progreso en Redis Pub/Sub.

    Rate-limited a 1 mensaje cada MIN_INTERVAL segundos para no saturar
    el canal. El progreso al 100% siempre se envía inmediatamente.
    """

    MIN_INTERVAL = 2.0  # segundos entre mensajes de progreso

    def __init__(self, task_id: str, tenant_id: str, file_id: str, channel: str) -> None:
        self.task_id = task_id
        self.tenant_id = tenant_id
        self.file_id = file_id
        self.channel = channel
        self._last_report: float = 0.0

    def report(
        self,
        percent: int,
        stage: str,
        stage_label: str,
        items_done: int = 0,
        items_total: int = 0,
        eta_seconds: Optional[int] = None,
    ) -> None:
        """
        Publica un mensaje de progreso en Redis.
        Rate-limited salvo cuando percent == 100.
        """
        now = time.time()
        is_final = percent >= 100
        if not is_final and (now - self._last_report) < self.MIN_INTERVAL:
            return

        payload = {
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "file_id": self.file_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": "completed" if is_final else "processing",
            "progress": {
                "percent": min(percent, 100),
                "stage": stage,
                "stage_label": stage_label,
                "items_done": items_done,
                "items_total": items_total,
            },
            "eta_seconds": eta_seconds,
        }

        serialized = json.dumps(payload)

        try:
            client = _get_client()
            client.publish(self.channel, serialized)
            # Persistir último estado para reconexiones (TTL 1 hora)
            client.setex(f"task:{self.task_id}:last_progress", 3600, serialized)
        except Exception as exc:
            # El progreso no es crítico — si Redis falla, el procesamiento continúa
            logger.warning("No se pudo publicar progreso en Redis", exc_info=exc)

        self._last_report = now

    def error(self, stage: str, error_message: str) -> None:
        """Publica un estado de error en el canal de progreso."""
        payload = {
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "file_id": self.file_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": "failed",
            "progress": {"percent": 0, "stage": stage, "stage_label": error_message},
        }
        serialized = json.dumps(payload)
        try:
            client = _get_client()
            client.publish(self.channel, serialized)
            client.setex(f"task:{self.task_id}:last_progress", 300, serialized)
        except Exception as exc:
            logger.warning("No se pudo publicar error en Redis", exc_info=exc)
