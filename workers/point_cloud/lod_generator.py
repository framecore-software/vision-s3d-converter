from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import laspy
import numpy as np
import open3d as o3d

logger = logging.getLogger(__name__)


@dataclass
class LodLevel:
    level: int
    label: str           # "high", "medium", "low", "preview"
    max_points: int      # Número máximo de puntos en este nivel


# Niveles de LOD por defecto
DEFAULT_LOD_LEVELS = [
    LodLevel(0, "high",    50_000_000),
    LodLevel(1, "medium",   5_000_000),
    LodLevel(2, "low",        500_000),
    LodLevel(3, "preview",     50_000),
]


def generate_lod(
    src: Path,
    output_dir: Path,
    levels: list[LodLevel] | None = None,
    output_format: str = "laz",
    on_progress: Callable[[int, str], None] | None = None,
) -> list[dict]:
    """
    Genera múltiples niveles de detalle de una nube de puntos.

    Estrategia:
    - Carga el archivo fuente con laspy (eficiente en memoria para LAS/LAZ)
    - Para cada nivel, aplica voxel downsampling con Open3D
    - Guarda cada nivel en el formato solicitado con laspy

    Args:
        src:           Archivo LAS/LAZ de entrada.
        output_dir:    Directorio donde se guardan los LOD.
        levels:        Lista de LodLevel. Si None usa DEFAULT_LOD_LEVELS.
        output_format: Extensión de salida: "laz" (default) o "las".
        on_progress:   Callback(percent, stage_label) para reportar avance.

    Returns:
        Lista de dicts con {level, label, path, point_count, size_bytes}.
    """
    levels = levels or DEFAULT_LOD_LEVELS
    output_dir.mkdir(parents=True, exist_ok=True)

    def _progress(pct: int, label: str) -> None:
        if on_progress:
            on_progress(pct, label)

    # ── Paso 1: Cargar puntos ────────────────────────────────────────
    _progress(5, "Leyendo archivo fuente...")
    logger.info("Cargando nube de puntos", extra={"src": src.name})

    las = laspy.read(src)
    xyz = np.vstack([las.x, las.y, las.z]).T.astype(np.float64)
    total_points = len(xyz)
    logger.info(f"Puntos cargados: {total_points:,}")

    # Extraer colores si existen (para preservarlos en los LOD)
    has_color = all(hasattr(las, dim) for dim in ("red", "green", "blue"))

    # ── Paso 2: Crear nube Open3D ────────────────────────────────────
    _progress(15, f"Procesando {total_points:,} puntos...")
    pcd = o3d.geometry.PointCloud()
    pcd.points = o3d.utility.Vector3dVector(xyz)

    if has_color:
        # Normalizar de 16-bit a [0, 1]
        colors = np.vstack([las.red, las.green, las.blue]).T / 65535.0
        pcd.colors = o3d.utility.Vector3dVector(colors)

    # ── Paso 3: Generar cada nivel de LOD ───────────────────────────
    results = []
    total_levels = len(levels)

    for i, lod in enumerate(levels):
        base_pct = 15 + int((i / total_levels) * 75)
        _progress(base_pct, f"Generando LOD {lod.label} (nivel {lod.level})...")

        if total_points <= lod.max_points:
            # Ya tiene menos puntos que el máximo: usar tal cual
            downsampled = pcd
            actual_count = total_points
        else:
            # Calcular voxel_size para aproximar el número de puntos objetivo
            # Estimamos el voxel_size iterativamente con una heurística
            bbox = pcd.get_axis_aligned_bounding_box()
            volume = np.prod(bbox.get_extent())
            target_density = lod.max_points / max(volume, 1e-9)
            voxel_size = (1.0 / target_density) ** (1.0 / 3.0)

            downsampled = pcd.voxel_down_sample(voxel_size=voxel_size)
            actual_count = len(downsampled.points)

            # Si quedamos muy por encima del objetivo, aplicar random sampling
            if actual_count > lod.max_points * 1.2:
                idx = np.random.choice(actual_count, lod.max_points, replace=False)
                downsampled = downsampled.select_by_index(idx.tolist())
                actual_count = len(downsampled.points)

        out_path = output_dir / f"lod_{lod.label}.{output_format.lstrip('.')}"
        _save_lod(las, downsampled, out_path, has_color)

        result = {
            "level":       lod.level,
            "label":       lod.label,
            "path":        out_path,
            "point_count": actual_count,
            "size_bytes":  out_path.stat().st_size,
        }
        results.append(result)

        _progress(base_pct + int(75 / total_levels), f"LOD {lod.label}: {actual_count:,} puntos")
        logger.info(
            "LOD generado",
            extra={"level": lod.label, "points": actual_count, "file": out_path.name},
        )

    _progress(95, "LOD completados")
    return results


def _save_lod(
    original_las: laspy.LasData,
    pcd: o3d.geometry.PointCloud,
    out_path: Path,
    has_color: bool,
) -> None:
    """Guarda una nube Open3D como archivo LAS/LAZ preservando el CRS original."""
    pts = np.asarray(pcd.points)

    new_las = laspy.LasData(header=laspy.LasHeader(
        point_format=original_las.header.point_format,
        version=original_las.header.version,
    ))
    # Preservar offsets y scales del original para mantener precisión geográfica
    new_las.header.offsets = original_las.header.offsets
    new_las.header.scales = original_las.header.scales

    new_las.x = pts[:, 0]
    new_las.y = pts[:, 1]
    new_las.z = pts[:, 2]

    if has_color and len(pcd.colors) > 0:
        colors = (np.asarray(pcd.colors) * 65535).astype(np.uint16)
        new_las.red   = colors[:, 0]
        new_las.green = colors[:, 1]
        new_las.blue  = colors[:, 2]

    compress = out_path.suffix.lower() == ".laz"
    new_las.write(str(out_path), do_compress=compress)
