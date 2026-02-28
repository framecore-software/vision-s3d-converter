from __future__ import annotations

from dataclasses import dataclass

from orchestrator.models.task import TaskType


@dataclass(frozen=True)
class TaskWeight:
    """
    Estimación de recursos que consume un tipo de tarea.

    cpu_percent    : % de CPU del servidor que consume (de 0 a 100)
    ram_gb         : GB de RAM que consume en pico
    concurrency_max: cuántas tareas de este tipo pueden correr en paralelo
    parallelizable : si puede coexistir con otras tareas pesadas
    """
    cpu_percent: float
    ram_gb: float
    concurrency_max: int
    parallelizable: bool = True


# ─────────────────────────────────────────────
# Tabla de pesos por tipo de tarea
#
# Los valores son estimaciones conservadoras para un servidor de 64 cores / 128GB RAM.
# El scheduler usa estas cifras para decidir si hay capacidad antes de lanzar un worker.
# Si en producción los valores reales difieren, ajustar aquí.
# ─────────────────────────────────────────────
TASK_WEIGHTS: dict[TaskType, TaskWeight] = {
    # ── Nubes de puntos ──────────────────────────────────────────────
    TaskType.POINT_CLOUD_LOD: TaskWeight(
        cpu_percent=75,
        ram_gb=32,
        concurrency_max=1,
        parallelizable=False,   # Tan pesada que no debe coexistir con otras pesadas
    ),
    TaskType.POINT_CLOUD_CONVERT: TaskWeight(
        cpu_percent=40,
        ram_gb=16,
        concurrency_max=2,
    ),

    # ── Mallas 3D ────────────────────────────────────────────────────
    TaskType.MESH_3D_LOD: TaskWeight(
        cpu_percent=60,
        ram_gb=24,
        concurrency_max=2,
    ),
    TaskType.MESH_3D_CONVERT: TaskWeight(
        cpu_percent=30,
        ram_gb=8,
        concurrency_max=4,
    ),

    # ── GeoTIFF ──────────────────────────────────────────────────────
    TaskType.GEOTIFF_RESIZE: TaskWeight(
        cpu_percent=35,
        ram_gb=12,
        concurrency_max=3,
    ),
    TaskType.GEOTIFF_COG: TaskWeight(
        cpu_percent=45,
        ram_gb=16,
        concurrency_max=2,
    ),

    # ── Imágenes ─────────────────────────────────────────────────────
    TaskType.IMAGE_CONVERT: TaskWeight(
        cpu_percent=5,
        ram_gb=1,
        concurrency_max=20,
    ),
    TaskType.IMAGE_THUMBNAIL: TaskWeight(
        cpu_percent=3,
        ram_gb=0.5,
        concurrency_max=30,
    ),

    # ── Video ────────────────────────────────────────────────────────
    TaskType.VIDEO_TRANSCODE: TaskWeight(
        cpu_percent=50,
        ram_gb=4,
        concurrency_max=3,
    ),

    # ── Archivos comprimidos ─────────────────────────────────────────
    TaskType.ARCHIVE_COMPRESS: TaskWeight(
        cpu_percent=20,
        ram_gb=2,
        concurrency_max=5,
    ),
    TaskType.ARCHIVE_EXTRACT: TaskWeight(
        cpu_percent=15,
        ram_gb=2,
        concurrency_max=5,
    ),
}


def get_weight(task_type: TaskType) -> TaskWeight:
    """Retorna el peso de un tipo de tarea. Lanza KeyError si no está registrado."""
    try:
        return TASK_WEIGHTS[task_type]
    except KeyError:
        raise KeyError(f"Sin peso definido para task_type='{task_type}'. Agregar en TASK_WEIGHTS.")


def is_heavy(task_type: TaskType) -> bool:
    """Considera 'pesada' cualquier tarea que usa ≥ 30% de CPU."""
    return TASK_WEIGHTS[task_type].cpu_percent >= 30
