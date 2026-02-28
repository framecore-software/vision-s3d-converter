from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import sys
from collections import deque
from pathlib import Path
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

    elif task.task_type in (TaskType.POINT_CLOUD_LOD, TaskType.POINT_CLOUD_CONVERT):
        from workers.point_cloud.worker import PointCloudWorker
        worker = PointCloudWorker(task)

    elif task.task_type in (TaskType.MESH_3D_LOD, TaskType.MESH_3D_CONVERT):
        from workers.mesh_3d.worker import Mesh3DWorker
        worker = Mesh3DWorker(task)

    elif task.task_type in (TaskType.GEOTIFF_RESIZE, TaskType.GEOTIFF_COG):
        from workers.geotiff.worker import GeoTiffWorker
        worker = GeoTiffWorker(task)

    elif task.task_type == TaskType.VIDEO_TRANSCODE:
        from workers.video.worker import VideoWorker
        worker = VideoWorker(task)

    elif task.task_type in (TaskType.ARCHIVE_COMPRESS, TaskType.ARCHIVE_EXTRACT):
        from workers.archive.worker import ArchiveWorker
        worker = ArchiveWorker(task)

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
        self._inflight: dict[str, tuple[Task, aio_pika.abc.AbstractIncomingMessage]] = {}
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._monitor = ResourceMonitor()
        self._pool = WorkerPool(on_complete=self._on_worker_complete)
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
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
        snapshot = await asyncio.get_running_loop().run_in_executor(
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
                self._inflight[task.task_id] = (task, message)
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
        outputs: list[TaskOutput] | None,
        error: str | None,
    ) -> None:
        """
        Llamado por WorkerPool cuando un subproceso termina.
        Publica el resultado y hace ACK/NACK en RabbitMQ.
        """
        inflight = self._inflight.pop(task_id, None)
        if inflight is None:
            logger.error("Mensaje no encontrado para task_id", extra={"task_id": task_id})
            return

        task, message = inflight
        # Scheduleamos las operaciones async desde este callback síncrono
        self._loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(
                self._finalize_task(task_id, success, outputs, error, message, task)
            )
        )

    async def _finalize_task(
        self,
        task_id: str,
        success: bool,
        outputs: list[TaskOutput] | None,
        error: str | None,
        message: aio_pika.abc.AbstractIncomingMessage,
        task: Task | None = None,
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
                # Intentar reencolar con backoff exponencial
                if task is not None:
                    retried = await rabbitmq_client.publish_retry(
                        self._connection, task, error or "unknown error"
                    )
                    if retried:
                        # ACK del mensaje original — el reintento ya está en la cola delayed
                        await message.ack()
                        logger.warning(
                            "Tarea fallida, reintento programado",
                            extra={"task_id": task_id, "attempt": task.retry.attempt, "error": error},
                        )
                        return

                # Sin task o sin más reintentos → notificar fallo permanente
                await rabbitmq_client.publish_result(channel, {
                    "task_id": task_id,
                    "status": "failed",
                    "error": error,
                })
                await message.ack()
                logger.error("Tarea fallida permanentemente", extra={"task_id": task_id, "error": error})

    # ─────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────

    @staticmethod
    def _is_supported(task_type: TaskType) -> bool:
        return task_type in (
            TaskType.IMAGE_CONVERT,
            TaskType.IMAGE_THUMBNAIL,
            TaskType.POINT_CLOUD_LOD,
            TaskType.POINT_CLOUD_CONVERT,
            TaskType.MESH_3D_LOD,
            TaskType.MESH_3D_CONVERT,
            TaskType.GEOTIFF_RESIZE,
            TaskType.GEOTIFF_COG,
            TaskType.VIDEO_TRANSCODE,
            TaskType.ARCHIVE_COMPRESS,
            TaskType.ARCHIVE_EXTRACT,
        )


# ─────────────────────────────────────────────
# Health check endpoint
# ─────────────────────────────────────────────

health_app = FastAPI(title="vs3d-orchestrator-health")

# Referencia al orchestrator para exponer métricas en /health
_orchestrator: Orchestrator | None = None


@health_app.get("/health")
async def health() -> JSONResponse:
    import psutil

    redis_ok = await redis_client.ping()
    cpu_pct = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()

    payload: dict = {
        "status":        "ok" if redis_ok else "degraded",
        "redis":         redis_ok,
        "pid":           os.getpid(),
        "cpu_percent":   cpu_pct,
        "ram_percent":   mem.percent,
        "ram_used_gb":   round(mem.used / 1024**3, 2),
        "ram_total_gb":  round(mem.total / 1024**3, 2),
    }

    if _orchestrator is not None:
        payload["pending_tasks"]  = len(_orchestrator._pending)
        payload["inflight_tasks"] = len(_orchestrator._inflight)
        payload["worker_count"]   = len(_orchestrator._pool._handles)

    http_code = 200 if redis_ok else 503
    return JSONResponse(payload, status_code=http_code)


@health_app.get("/metrics")
async def metrics() -> JSONResponse:
    """Métricas detalladas para monitoreo (Prometheus-ready en formato JSON)."""
    import psutil

    cpu_per_core = psutil.cpu_percent(percpu=True, interval=None)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    return JSONResponse({
        "cpu_percent_total":    psutil.cpu_percent(interval=None),
        "cpu_percent_per_core": cpu_per_core,
        "cpu_count":            psutil.cpu_count(),
        "ram_total_gb":         round(mem.total / 1024**3, 2),
        "ram_used_gb":          round(mem.used / 1024**3, 2),
        "ram_percent":          mem.percent,
        "disk_total_gb":        round(disk.total / 1024**3, 2),
        "disk_used_gb":         round(disk.used / 1024**3, 2),
        "disk_percent":         disk.percent,
        "pending_tasks":        len(_orchestrator._pending) if _orchestrator else 0,
        "inflight_tasks":       len(_orchestrator._inflight) if _orchestrator else 0,
        "worker_count":         len(_orchestrator._pool._handles) if _orchestrator else 0,
    })


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

async def _main() -> None:
    global _orchestrator
    orchestrator = Orchestrator()
    _orchestrator = orchestrator

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
