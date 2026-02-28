from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import sys
import time
from pathlib import Path
from typing import Any

import aio_pika
import aio_pika.abc
import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from orchestrator.config import settings
from orchestrator.models.task import Task, TaskResult, TaskStatus, TaskType
from orchestrator.queue import rabbitmq_client, redis_client
from orchestrator.queue.rabbitmq_client import publish_result

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

def _get_worker_class(task_type: TaskType):
    """Retorna la clase worker correspondiente al tipo de tarea."""
    if task_type in (TaskType.IMAGE_CONVERT, TaskType.IMAGE_THUMBNAIL):
        from workers.image.worker import ImageWorker
        return ImageWorker
    # Fase 4+: agregar aquí los demás workers
    raise NotImplementedError(f"Worker no implementado para: {task_type}")


# ─────────────────────────────────────────────
# Ejecución de worker en subproceso
# ─────────────────────────────────────────────

def _run_worker_subprocess(task_dict: dict) -> dict:
    """
    Función que corre en el subproceso hijo.
    Importa el worker, lo ejecuta y retorna el resultado serializado.
    """
    task = Task.model_validate(task_dict)
    worker_class = _get_worker_class(task.task_type)
    worker = worker_class(task)

    try:
        outputs = worker.run()
        return {
            "status": "completed",
            "outputs": [o.model_dump() for o in outputs],
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": str(exc),
        }


# ─────────────────────────────────────────────
# Orchestrator principal
# ─────────────────────────────────────────────

class Orchestrator:
    def __init__(self) -> None:
        self._active_processes: dict[str, multiprocessing.Process] = {}
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None

    async def start(self) -> None:
        logger.info("Iniciando orchestrator", extra={"pid": os.getpid()})
        Path(settings.checkpoint_dir).mkdir(parents=True, exist_ok=True)
        Path(settings.tmp_processing_dir).mkdir(parents=True, exist_ok=True)

        self._connection = await rabbitmq_client.connect()
        await rabbitmq_client.consume_tasks(self._connection, self._handle_task)
        logger.info("Orchestrator listo, esperando tareas")

        # Mantener el proceso vivo
        while True:
            await asyncio.sleep(5)
            self._reap_finished_processes()

    async def _handle_task(
        self,
        task: Task,
        message: aio_pika.abc.AbstractIncomingMessage,
    ) -> None:
        """
        Recibe una tarea de RabbitMQ y lanza un subproceso para procesarla.
        El ACK se hace cuando el subproceso termina exitosamente.
        """
        logger.info(
            "Tarea recibida",
            extra={"task_id": task.task_id, "task_type": task.task_type, "tenant_id": task.tenant_id},
        )

        # Verificar que el tipo de tarea está soportado
        try:
            _get_worker_class(task.task_type)
        except NotImplementedError:
            logger.warning("Tipo de tarea no soportado aún", extra={"task_type": task.task_type})
            await message.nack(requeue=True)  # Requeue para cuando esté implementado
            return

        # Lanzar worker en subproceso aislado
        ctx = multiprocessing.get_context("spawn")
        result_queue: multiprocessing.Queue = ctx.Queue()

        def _worker_wrapper(task_dict: dict, q: multiprocessing.Queue) -> None:
            result = _run_worker_subprocess(task_dict)
            q.put(result)

        process = ctx.Process(
            target=_worker_wrapper,
            args=(task.model_dump(mode="json"), result_queue),
            daemon=False,
            name=f"worker-{task.task_id[:8]}",
        )
        process.start()
        logger.info("Worker lanzado", extra={"task_id": task.task_id, "pid": process.pid})

        # Esperar resultado de forma async sin bloquear el event loop
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: result_queue.get(timeout=7200)  # 2h max
        )
        process.join()

        # Publicar resultado y hacer ACK/NACK
        async with self._connection.channel() as channel:
            if result["status"] == "completed":
                await publish_result(channel, {
                    "task_id": task.task_id,
                    "tenant_id": task.tenant_id,
                    "file_id": task.file_id,
                    "status": "completed",
                    "outputs": result["outputs"],
                })
                await message.ack()
                logger.info("Tarea completada", extra={"task_id": task.task_id})
            else:
                attempt = task.retry.attempt
                max_attempts = task.retry.max_attempts

                if attempt < max_attempts:
                    # Republicar con attempt+1 para reintento
                    task.retry.attempt += 1
                    task.retry.previous_error = result.get("error", "unknown")
                    await rabbitmq_client.publish_task(self._connection, task)
                    logger.warning(
                        "Tarea fallida, reintentando",
                        extra={"task_id": task.task_id, "attempt": attempt + 1},
                    )
                else:
                    logger.error(
                        "Tarea fallida definitivamente, enviando a DLQ",
                        extra={"task_id": task.task_id, "error": result.get("error")},
                    )
                    await publish_result(channel, {
                        "task_id": task.task_id,
                        "tenant_id": task.tenant_id,
                        "file_id": task.file_id,
                        "status": "failed",
                        "error": result.get("error"),
                    })

                await message.ack()  # ACK para evitar requeue infinito desde RabbitMQ

    def _reap_finished_processes(self) -> None:
        finished = [pid for pid, p in self._active_processes.items() if not p.is_alive()]
        for pid in finished:
            del self._active_processes[pid]


# ─────────────────────────────────────────────
# Health check endpoint (FastAPI mínimo)
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

    # Correr health server y orchestrator concurrentemente
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
