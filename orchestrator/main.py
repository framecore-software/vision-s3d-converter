from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import sys
from collections import deque
from typing import Any

import aio_pika
import aio_pika.abc
import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from orchestrator.config import settings
from orchestrator.models.task import Task, TaskOutput, TaskType
from orchestrator.queue import rabbitmq_client, redis_client
from orchestrator.scheduler.bin_packer import pack
from orchestrator.scheduler.resource_monitor import ResourceMonitor
from orchestrator.scheduler.worker_pool import WorkerPool
from pathlib import Path

# ─────────────────────────────────────────────
# Logging estructurado
# ─────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    stream=sys.stdout,
    format="%(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Registro de workers disponibles
# ─────────────────────────────────────────────

def _run_worker_subprocess(task_dict: dict) -> dict:
    """
    Corre en el subproceso hijo.
    Importa el worker correcto, lo ejecuta y devuelve el resultado.
    """
    task = Task.model_validate(task_dict)

    if task.task_type in (TaskType.IMAGE_CONVERT, TaskType.IMAGE_THUMBNAIL):
        from workers.image.worker import ImageWorker
        worker = ImageWorker(task)
    else:
        return {"status": "failed", "error": f"Worker no implementado: {task.task_type}"}

    try:
        outputs = worker.run()
        return {"status": "completed", "outputs": [o.model_dump() for o in outputs]}
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


# ─────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────

class Orchestrator:
    def __init__(self) -> None:
        self._pending: deque[tuple[Task, aio_pika.abc.AbstractIncomingMessage]] = deque()
        self._inflight: dict[str, aio_pika.abc.AbstractIncomingMessage] = {}
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._monitor = ResourceMonitor()
        self._pool = WorkerPool(on_complete=self._on_worker_complete)

    async def start(self) -> None:
        logger.info("Iniciando orchestrator", extra={"pid": os.getpid()})
        Path(settings.checkpoint_dir).mkdir(parents=True, exist_ok=True)
        Path(settings.tmp_processing_dir).mkdir(parents=True, exist_ok=True)

        self._connection = await rabbitmq_client.connect()
        await rabbitmq_client.consume_tasks(self._connection, self._enqueue_task)
        logger.info("Orchestrator listo, esperando tareas")

        # Loop principal del scheduler
        while True:
            await self._scheduler_cycle()
            await asyncio.sleep(settings.scheduler_cycle_seconds)

    # ─────────────────────────────────────────────
    # Recepción de mensajes desde RabbitMQ
    # ─────────────────────────────────────────────

    async def _enqueue_task(
        self,
        task: Task,
        message: aio_pika.abc.AbstractIncomingMessage,
    ) -> None:
        """
        Recibe un mensaje de RabbitMQ y lo encola localmente.
        NO hace ACK todavía — el ACK se hace después de que el worker termina.
        """
        if not self._is_supported(task.task_type):
            logger.warning(
                "Tipo de tarea no soportado aún, requeue",
                extra={"task_type": task.task_type},
            )
            await message.nack(requeue=True)
            return

        self._pending.append((task, message))
        logger.info(
            "Tarea encolada",
            extra={
                "task_id": task.task_id,
                "task_type": task.task_type,
                "queue_length": len(self._pending),
            },
        )

    # ─────────────────────────────────────────────
    # Ciclo del scheduler (corre cada N segundos)
    # ─────────────────────────────────────────────

    async def _scheduler_cycle(self) -> None:
        # 1. Recoger workers terminados y procesar sus resultados
        self._pool.reap()

        if not self._pending:
            return

        # 2. Tomar snapshot de recursos (bloqueante ~2s en executor para no bloquear el loop)
        snapshot = await asyncio.get_event_loop().run_in_executor(
            None, self._monitor.refresh
        )

        # 3. Decidir qué tareas lanzar con el bin-packer
        pending_tasks = [task for task, _ in self._pending]
        to_launch = pack(
            pending_tasks=pending_tasks,
            snapshot=snapshot,
            already_running_types=self._pool.active_task_types(),
        )

        if not to_launch:
            return

        # 4. Lanzar los workers seleccionados
        launch_ids = {t.task_id for t in to_launch}
        new_pending: deque = deque()

        for task, message in self._pending:
            if task.task_id in launch_ids:
                self._inflight[task.task_id] = message
                self._pool.launch(task, _run_worker_subprocess)
            else:
                new_pending.append((task, message))

        self._pending = new_pending

    # ─────────────────────────────────────────────
    # Callback: worker terminado
    # ─────────────────────────────────────────────

    def _on_worker_complete(
        self,
        task_id: str,
        success: bool,
        outputs: list | None,
        error: str | None,
    ) -> None:
        """
        Llamado por WorkerPool cuando un subproceso termina.
        Publica el resultado y hace ACK/NACK en RabbitMQ.
        """
        message = self._inflight.pop(task_id, None)
        if message is None:
            logger.error("Mensaje no encontrado para task_id", extra={"task_id": task_id})
            return

        # Scheduleamos las operaciones async desde este callback síncrono
        asyncio.get_event_loop().call_soon_threadsafe(
            lambda: asyncio.ensure_future(
                self._finalize_task(task_id, success, outputs, error, message)
            )
        )

    async def _finalize_task(
        self,
        task_id: str,
        success: bool,
        outputs: list | None,
        error: str | None,
        message: aio_pika.abc.AbstractIncomingMessage,
    ) -> None:
        async with self._connection.channel() as channel:
            if success:
                await rabbitmq_client.publish_result(channel, {
                    "task_id": task_id,
                    "status": "completed",
                    "outputs": outputs or [],
                })
                await message.ack()
                logger.info("Tarea completada y confirmada", extra={"task_id": task_id})
            else:
                # Buscar la tarea original en inflight ya no sirve (fue removida),
                # pero el mensaje tiene los headers de retry de RabbitMQ
                await rabbitmq_client.publish_result(channel, {
                    "task_id": task_id,
                    "status": "failed",
                    "error": error,
                })
                await message.ack()
                logger.error("Tarea fallida notificada", extra={"task_id": task_id, "error": error})

    # ─────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────

    @staticmethod
    def _is_supported(task_type: TaskType) -> bool:
        return task_type in (TaskType.IMAGE_CONVERT, TaskType.IMAGE_THUMBNAIL)


# ─────────────────────────────────────────────
# Health check endpoint
# ─────────────────────────────────────────────

health_app = FastAPI(title="vs3d-orchestrator-health")


@health_app.get("/health")
async def health() -> JSONResponse:
    redis_ok = await redis_client.ping()
    return JSONResponse({
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "pid": os.getpid(),
    })


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

async def _main() -> None:
    orchestrator = Orchestrator()

    health_config = uvicorn.Config(
        health_app,
        host="0.0.0.0",
        port=settings.health_port,
        log_level="warning",
    )
    health_server = uvicorn.Server(health_config)

    await asyncio.gather(
        orchestrator.start(),
        health_server.serve(),
    )


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    asyncio.run(_main())
