from __future__ import annotations

import json
import logging
import os
import signal
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from orchestrator.config import settings
from orchestrator.models.task import Task, TaskOutput, TaskStatus
from progress.reporter import ProgressReporter

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Clase base para todos los workers de procesamiento.

    Responsabilidades:
    - Gestionar el directorio de trabajo temporal
    - Checkpoint: persistir y restaurar estado entre reintentos
    - Manejo de SIGTERM: guardar checkpoint antes de morir
    - Reportar progreso vía Redis Pub/Sub
    - Template method: download → process → upload → cleanup
    """

    def __init__(self, task: Task) -> None:
        self.task = task
        self.task_id = task.task_id
        self.progress = ProgressReporter(
            task_id=task.task_id,
            tenant_id=task.tenant_id,
            file_id=task.file_id,
            channel=task.webhook.progress_channel,
        )

        # Directorio de trabajo temporal exclusivo para esta tarea
        self.work_dir = Path(settings.tmp_processing_dir) / task.task_id
        self.work_dir.mkdir(parents=True, exist_ok=True)

        # Checkpoint
        self._checkpoint_path = Path(settings.checkpoint_dir) / f"{task.task_id}.json"
        self._terminated = False
        self._start_time = time.monotonic()

        # Registrar handler SIGTERM para guardar checkpoint al ser matado
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    # ─────────────────────────────────────────────
    # API pública
    # ─────────────────────────────────────────────

    def run(self) -> list[TaskOutput]:
        """
        Punto de entrada principal. Orquesta el flujo completo:
        checkpoint existente → process → limpieza.
        """
        checkpoint = self._load_checkpoint()
        resume_state = checkpoint.get("state") if checkpoint else None

        if resume_state:
            logger.info(
                "Retomando desde checkpoint",
                extra={"task_id": self.task_id, "stage": resume_state.get("stage")},
            )

        try:
            outputs = self.process(resume_state=resume_state)
            self._clear_checkpoint()
            return outputs
        except SystemExit:
            # SIGTERM recibido, checkpoint ya guardado en _handle_sigterm
            raise
        except Exception:
            logger.exception("Error en worker", extra={"task_id": self.task_id})
            raise
        finally:
            self._cleanup_work_dir()

    @abstractmethod
    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        """
        Implementación específica del procesamiento.
        Debe llamar a self.progress.report() periódicamente.
        Debe llamar a self.save_checkpoint() después de cada etapa larga.
        Retorna lista de TaskOutput con los archivos generados.
        """
        ...

    # ─────────────────────────────────────────────
    # Checkpoint
    # ─────────────────────────────────────────────

    def save_checkpoint(self, state: dict[str, Any]) -> None:
        """Persiste el estado actual en disco para poder retomar si el worker muere."""
        checkpoint = {
            "task_id": self.task_id,
            "task_type": self.task.task_type,
            "state": state,
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "elapsed_seconds": time.monotonic() - self._start_time,
        }
        self._checkpoint_path.write_text(json.dumps(checkpoint, indent=2))
        logger.debug("Checkpoint guardado", extra={"task_id": self.task_id, "stage": state.get("stage")})

    def _load_checkpoint(self) -> dict[str, Any] | None:
        if self._checkpoint_path.exists():
            try:
                return json.loads(self._checkpoint_path.read_text())
            except Exception:
                logger.warning("Checkpoint corrupto, ignorando", extra={"task_id": self.task_id})
        return None

    def _clear_checkpoint(self) -> None:
        self._checkpoint_path.unlink(missing_ok=True)

    # ─────────────────────────────────────────────
    # Limpieza
    # ─────────────────────────────────────────────

    def _cleanup_work_dir(self) -> None:
        """Elimina el directorio de trabajo temporal al finalizar."""
        import shutil
        if self.work_dir.exists():
            shutil.rmtree(self.work_dir, ignore_errors=True)
            logger.debug("Directorio temporal limpiado", extra={"task_id": self.task_id})

    # ─────────────────────────────────────────────
    # Utilidades para subclases
    # ─────────────────────────────────────────────

    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start_time

    def work_path(self, filename: str) -> Path:
        """Retorna una ruta dentro del directorio de trabajo temporal."""
        return self.work_dir / filename

    # ─────────────────────────────────────────────
    # SIGTERM handler
    # ─────────────────────────────────────────────

    def _handle_sigterm(self, signum: int, frame: Any) -> None:
        logger.warning("SIGTERM recibido, guardando checkpoint", extra={"task_id": self.task_id})
        self._terminated = True
        self.save_checkpoint({"stage": "interrupted", "reason": "sigterm"})
        raise SystemExit(0)
