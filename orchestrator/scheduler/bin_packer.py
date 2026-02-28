from __future__ import annotations

import logging
from dataclasses import dataclass, field

from orchestrator.models.task import Task, TaskType
from orchestrator.scheduler.resource_monitor import ResourceSnapshot
from orchestrator.scheduler.task_weights import TaskWeight, get_weight, is_heavy

logger = logging.getLogger(__name__)


@dataclass
class SchedulingDecision:
    can_run: bool
    reason: str
    # Score de utilización (0.0 - 1.0): cuánto aprovecha el espacio disponible.
    # Mayor score = mejor candidato cuando hay que elegir entre varias tareas.
    utilization_score: float = 0.0


@dataclass
class VirtualCapacity:
    """
    Capacidad disponible simulada durante un ciclo de scheduling.
    Se va reduciendo a medida que se "reservan" recursos para tareas a lanzar,
    sin esperar a que psutil refleje los procesos recién iniciados.
    """
    cpu_available: float
    ram_available_gb: float
    active_types: list[TaskType] = field(default_factory=list)

    def reserve(self, weight: TaskWeight, task_type: TaskType) -> None:
        self.cpu_available -= weight.cpu_percent
        self.ram_available_gb -= weight.ram_gb
        self.active_types.append(task_type)

    def fits(self, weight: TaskWeight) -> bool:
        return (
            weight.cpu_percent <= self.cpu_available
            and weight.ram_gb <= self.ram_available_gb
        )


def _check_concurrency(task_type: TaskType, weight: TaskWeight, capacity: VirtualCapacity) -> bool:
    """True si no se ha alcanzado el límite de concurrencia para este tipo."""
    current = capacity.active_types.count(task_type)
    return current < weight.concurrency_max


def _check_parallelism(weight: TaskWeight, capacity: VirtualCapacity) -> bool:
    """
    Si la tarea no es paralelizable (parallelizable=False), solo puede correr
    si no hay ninguna otra tarea no-paralelizable activa.
    """
    if weight.parallelizable:
        return True
    # Verificar si ya hay alguna tarea no-paralelizable corriendo
    for active_type in capacity.active_types:
        active_weight = get_weight(active_type)
        if not active_weight.parallelizable:
            return False
    return True


def evaluate(
    task: Task,
    capacity: VirtualCapacity,
) -> SchedulingDecision:
    """
    Evalúa si una tarea puede ser lanzada dada la capacidad virtual disponible.
    Retorna una SchedulingDecision con el motivo y el score de utilización.
    """
    weight = get_weight(task.task_type)

    if not _check_concurrency(task.task_type, weight, capacity):
        return SchedulingDecision(False, "concurrency_limit")

    if not _check_parallelism(weight, capacity):
        return SchedulingDecision(False, "parallelism_conflict")

    if not capacity.fits(weight):
        if weight.cpu_percent > capacity.cpu_available:
            return SchedulingDecision(False, "cpu_constrained")
        return SchedulingDecision(False, "ram_constrained")

    # Calcular score: qué fracción del espacio disponible aprovecha esta tarea.
    # Combinamos CPU (60%) y RAM (40%) para el score final.
    cpu_score = weight.cpu_percent / max(capacity.cpu_available, 1)
    ram_score = weight.ram_gb / max(capacity.ram_available_gb, 0.1)
    score = cpu_score * 0.6 + ram_score * 0.4

    return SchedulingDecision(True, "fits", utilization_score=score)


# Límite global absoluto de subprocesos worker activos simultáneamente.
# Independiente de los concurrency_max por tipo de tarea — protege contra
# escenarios donde la suma de todos los concurrency_max supera lo razonable
# (e.g. 20 image_convert + 30 image_thumbnail = 50 procesos).
MAX_CONCURRENT_WORKERS = 50


def pack(
    pending_tasks: list[Task],
    snapshot: ResourceSnapshot,
    already_running_types: list[TaskType],
) -> list[Task]:
    """
    Algoritmo de bin-packing adaptado para scheduling de tareas.

    Estrategia First-Fit Decreasing (FFD) en dos pasadas:
      1. Pasada pesada: tareas con CPU ≥ 30%, ordenadas por prioridad+FIFO.
         Se elige la más grande que quepa (mayor score de utilización).
      2. Pasada ligera: rellena el espacio restante con tareas pequeñas.
         También ordenadas por prioridad+FIFO.

    Args:
        pending_tasks:         lista de tareas en cola, ya filtradas (no en ejecución)
        snapshot:              lectura actual de recursos del servidor
        already_running_types: tipos de tarea actualmente en ejecución

    Returns:
        Lista de tareas que deben lanzarse en este ciclo.
    """
    # Verificar límite global de concurrencia antes de empezar
    current_total = len(already_running_types)
    if current_total >= MAX_CONCURRENT_WORKERS:
        logger.debug(
            "Límite global de workers alcanzado, ciclo de scheduling vacío",
            extra={"current": current_total, "max": MAX_CONCURRENT_WORKERS},
        )
        return []

    capacity = VirtualCapacity(
        cpu_available=snapshot.cpu_available,
        ram_available_gb=snapshot.ram_available_target_gb,
        active_types=list(already_running_types),
    )

    if capacity.cpu_available <= 0 or capacity.ram_available_gb <= 0:
        logger.debug("Sin capacidad disponible, ciclo de scheduling vacío")
        return []

    # Ordenar: prioridad DESC (10=máxima), submitted_at ASC (FIFO dentro del mismo nivel)
    sorted_tasks = sorted(
        pending_tasks,
        key=lambda t: (-t.priority, t.submitted_at),
    )

    heavy = [t for t in sorted_tasks if is_heavy(t.task_type)]
    light = [t for t in sorted_tasks if not is_heavy(t.task_type)]

    to_launch: list[Task] = []

    # Cuántos workers nuevos podemos lanzar en este ciclo sin superar el cap global
    slots_available = MAX_CONCURRENT_WORKERS - current_total

    # ── Pasada 1: tareas pesadas ─────────────────────────────────────
    # Para cada posición en la cola (respetando FIFO por prioridad),
    # evaluamos si cabe. Si no cabe la primera, seguimos buscando una
    # más pequeña que sí quepa (bin-packing).
    scheduled_ids = set()
    for task in heavy:
        if len(to_launch) >= slots_available:
            break
        decision = evaluate(task, capacity)
        if decision.can_run:
            to_launch.append(task)
            scheduled_ids.add(task.task_id)
            capacity.reserve(get_weight(task.task_type), task.task_type)
            logger.debug(
                "Tarea pesada programada",
                extra={
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "score": round(decision.utilization_score, 3),
                    "cpu_restante": round(capacity.cpu_available, 1),
                    "ram_restante_gb": round(capacity.ram_available_gb, 1),
                },
            )

        # Parar si ya no hay recursos para ninguna tarea pesada mínima
        if capacity.cpu_available < 30 and capacity.ram_available_gb < 4:
            break

    # ── Pasada 2: tareas ligeras (relleno de bins) ───────────────────
    for task in light:
        if len(to_launch) >= slots_available:
            break
        if task.task_id in scheduled_ids:
            continue
        decision = evaluate(task, capacity)
        if decision.can_run:
            to_launch.append(task)
            scheduled_ids.add(task.task_id)
            capacity.reserve(get_weight(task.task_type), task.task_type)
            logger.debug(
                "Tarea ligera programada",
                extra={
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "cpu_restante": round(capacity.cpu_available, 1),
                },
            )

        # Parar si ya no cabe ni la tarea más ligera posible
        if capacity.cpu_available < 3 or capacity.ram_available_gb < 0.5:
            break

    if to_launch:
        logger.info(
            "Ciclo de scheduling completado",
            extra={
                "tareas_programadas": len(to_launch),
                "cpu_reservado": round(snapshot.cpu_available - capacity.cpu_available, 1),
                "ram_reservada_gb": round(snapshot.ram_available_target_gb - capacity.ram_available_gb, 1),
            },
        )

    return to_launch
