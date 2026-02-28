from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import psutil

from orchestrator.config import settings

logger = logging.getLogger(__name__)


@dataclass
class ResourceSnapshot:
    """Estado actual de recursos del servidor host."""
    cpu_percent: float          # % de CPU usado (promedio todos los cores)
    ram_percent: float          # % de RAM usada
    ram_used_gb: float          # GB de RAM en uso
    ram_available_gb: float     # GB de RAM disponible
    cpu_available: float        # % de CPU disponible hasta el target
    ram_available_target_gb: float  # GB disponibles hasta el target


def take_snapshot(cpu_interval: float = 2.0) -> ResourceSnapshot:
    """
    Toma una lectura real de CPU y RAM del servidor host.

    cpu_interval controla el tiempo de muestreo de psutil.
    Con interval=2.0 bloquea 2 segundos pero da una lectura precisa.
    Usar interval=None para lectura instantánea (menos precisa).
    """
    cpu = psutil.cpu_percent(interval=cpu_interval)
    ram = psutil.virtual_memory()
    ram_used_gb = ram.used / (1024 ** 3)
    ram_available_gb = ram.available / (1024 ** 3)
    ram_total_gb = ram.total / (1024 ** 3)

    cpu_available = max(0.0, settings.max_cpu_percent - cpu)
    ram_target_gb = ram_total_gb * (settings.max_ram_percent / 100)
    ram_available_target_gb = max(0.0, ram_target_gb - ram_used_gb)

    snapshot = ResourceSnapshot(
        cpu_percent=cpu,
        ram_percent=ram.percent,
        ram_used_gb=ram_used_gb,
        ram_available_gb=ram_available_gb,
        cpu_available=cpu_available,
        ram_available_target_gb=ram_available_target_gb,
    )

    logger.debug(
        "Resource snapshot",
        extra={
            "cpu_percent": cpu,
            "ram_percent": ram.percent,
            "cpu_available": cpu_available,
            "ram_available_gb": round(ram_available_target_gb, 1),
        },
    )
    return snapshot


def is_above_target(snapshot: ResourceSnapshot) -> bool:
    """True si algún recurso supera el target configurado."""
    return (
        snapshot.cpu_percent >= settings.max_cpu_percent
        or snapshot.ram_percent >= settings.max_ram_percent
    )


class ResourceMonitor:
    """
    Monitor que mantiene un snapshot actualizado en background.

    En lugar de bloquear 2 segundos cada vez que el scheduler necesita
    datos, corre una tarea asyncio que actualiza el snapshot periódicamente.
    """

    def __init__(self) -> None:
        self._snapshot: ResourceSnapshot | None = None
        self._last_update: float = 0.0

    def get_snapshot(self) -> ResourceSnapshot:
        """
        Retorna el snapshot más reciente.
        Si no hay uno disponible, toma uno en el momento (bloqueante 1s).
        """
        if self._snapshot is None:
            self._snapshot = take_snapshot(cpu_interval=1.0)
            self._last_update = time.monotonic()
        return self._snapshot

    def refresh(self) -> ResourceSnapshot:
        """Fuerza una lectura fresca (bloqueante ~2s)."""
        self._snapshot = take_snapshot(cpu_interval=2.0)
        self._last_update = time.monotonic()
        return self._snapshot

    @property
    def age_seconds(self) -> float:
        return time.monotonic() - self._last_update
