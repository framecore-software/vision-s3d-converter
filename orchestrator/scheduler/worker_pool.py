from __future__ import annotations

import logging
import multiprocessing
import multiprocessing.queues
import os
import queue
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from orchestrator.models.task import Task, TaskOutput, TaskType

logger = logging.getLogger(__name__)

# Callback que el pool llama cuando un worker termina
OnCompleteCallback = Callable[[str, bool, list[TaskOutput] | None, str | None], None]
# on_complete(task_id, success, outputs, error_message)


@dataclass
class WorkerHandle:
    """Referencia a un subproceso worker activo."""
    task_id: str
    task_type: TaskType
    process: multiprocessing.Process
    result_queue: multiprocessing.Queue
    started_at: float = field(default_factory=time.monotonic)

    @property
    def pid(self) -> int | None:
        return self.process.pid

    @property
    def is_alive(self) -> bool:
        return self.process.is_alive()

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.started_at


class WorkerPool:
    """
    Gestiona el ciclo de vida de los subprocesos worker.

    Responsabilidades:
    - Lanzar workers como subprocesos independientes (spawn)
    - Rastrear qué tipos de tarea están activos (para el bin-packer)
    - Recolectar resultados de procesos terminados
    - Matar procesos que superen el timeout máximo
    """

    MAX_TASK_TIMEOUT_SECONDS = 7_200  # 2 horas

    def __init__(self, on_complete: OnCompleteCallback) -> None:
        self._handles: dict[str, WorkerHandle] = {}  # task_id → handle
        self._on_complete = on_complete
        self._ctx = multiprocessing.get_context("spawn")

    # ─────────────────────────────────────────────
    # Lanzar worker
    # ─────────────────────────────────────────────

    def launch(self, task: Task, worker_fn: Callable[[dict], dict]) -> WorkerHandle:
        """
        Lanza un worker en un subproceso independiente.

        worker_fn debe aceptar un dict (task serializado) y retornar un dict con:
            {"status": "completed", "outputs": [...]}
            {"status": "failed",    "error": "..."}
        """
        result_queue: multiprocessing.Queue = self._ctx.Queue()

        def _wrapper(task_dict: dict, q: multiprocessing.Queue) -> None:
            from orchestrator.config import settings
            result = worker_fn(task_dict)
            # Poner el resultado en la queue ANTES de limpiar el checkpoint.
            # Garantiza que si el proceso muere entre estas dos operaciones,
            # el orchestrator recibe el resultado y no reintenta desde un
            # checkpoint obsoleto. Orden: resultado → limpieza (no al revés).
            q.put(result)
            if result.get("status") == "completed":
                checkpoint = Path(settings.checkpoint_dir) / f"{task_dict['task_id']}.json"
                checkpoint.unlink(missing_ok=True)

        process = self._ctx.Process(
            target=_wrapper,
            args=(task.model_dump(mode="json"), result_queue),
            daemon=False,
            name=f"worker-{task.task_id[:8]}",
        )
        process.start()

        handle = WorkerHandle(
            task_id=task.task_id,
            task_type=task.task_type,
            process=process,
            result_queue=result_queue,
        )
        self._handles[task.task_id] = handle

        logger.info(
            "Worker lanzado",
            extra={"task_id": task.task_id, "task_type": task.task_type, "pid": process.pid},
        )
        return handle

    # ─────────────────────────────────────────────
    # Recolectar procesos terminados
    # ─────────────────────────────────────────────

    def reap(self) -> list[tuple[str, bool, list | None, str | None]]:
        """
        Recoge resultados de workers terminados y los elimina del pool.
        Retorna lista de (task_id, success, outputs, error).
        Llama a on_complete por cada worker finalizado.
        """
        finished = []
        to_remove = []

        for task_id, handle in self._handles.items():
            # Verificar timeout
            if handle.elapsed_seconds > self.MAX_TASK_TIMEOUT_SECONDS:
                logger.error(
                    "Worker superó timeout, matando proceso",
                    extra={"task_id": task_id, "elapsed": handle.elapsed_seconds},
                )
                handle.process.kill()
                handle.process.join(timeout=5)
                finished.append((task_id, False, None, "timeout"))
                to_remove.append(task_id)
                continue

            if not handle.is_alive:
                result = self._collect_result(handle)
                finished.append(result)
                to_remove.append(task_id)

        for task_id in to_remove:
            del self._handles[task_id]

        for task_id, success, outputs, error in finished:
            self._on_complete(task_id, success, outputs, error)

        return finished

    def _collect_result(
        self, handle: WorkerHandle
    ) -> tuple[str, bool, list | None, str | None]:
        handle.process.join(timeout=5)
        try:
            result = handle.result_queue.get_nowait()
        except queue.Empty:
            # El proceso terminó sin poner nada en la cola (crash, OOM, etc.)
            exit_code = handle.process.exitcode
            error = f"Worker terminó sin resultado (exit code {exit_code})"
            logger.error(error, extra={"task_id": handle.task_id, "exit_code": exit_code})
            return handle.task_id, False, None, error

        if result.get("status") == "completed":
            logger.info(
                "Worker completado",
                extra={"task_id": handle.task_id, "elapsed": round(handle.elapsed_seconds)},
            )
            return handle.task_id, True, result.get("outputs"), None
        else:
            error = result.get("error", "unknown error")
            logger.warning(
                "Worker fallido",
                extra={"task_id": handle.task_id, "error": error},
            )
            return handle.task_id, False, None, error

    # ─────────────────────────────────────────────
    # Consultas de estado (usadas por el bin-packer)
    # ─────────────────────────────────────────────

    def active_task_types(self) -> list[TaskType]:
        """Lista de TaskType actualmente en ejecución (con repeticiones)."""
        return [h.task_type for h in self._handles.values() if h.is_alive]

    def active_count(self) -> int:
        return len(self._handles)

    def is_running(self, task_id: str) -> bool:
        return task_id in self._handles

    def kill(self, task_id: str) -> bool:
        """Mata un worker específico (SIGKILL). El checkpoint ya fue guardado vía SIGTERM."""
        handle = self._handles.get(task_id)
        if not handle:
            return False
        logger.warning("Matando worker", extra={"task_id": task_id, "pid": handle.pid})
        handle.process.kill()
        return True
