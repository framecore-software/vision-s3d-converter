from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import open3d as o3d

logger = logging.getLogger(__name__)


@dataclass
class MeshLodLevel:
    level: int
    label: str
    target_triangles: int    # Número objetivo de triángulos


DEFAULT_MESH_LOD_LEVELS = [
    MeshLodLevel(0, "high",    0),          # 0 = sin reducción (original)
    MeshLodLevel(1, "medium",  500_000),
    MeshLodLevel(2, "low",      50_000),
    MeshLodLevel(3, "preview",   5_000),
]


def generate_mesh_lod(
    src: Path,
    output_dir: Path,
    levels: list[MeshLodLevel] | None = None,
    output_format: str = "glb",
    on_progress: Callable[[int, str], None] | None = None,
) -> list[dict]:
    """
    Genera múltiples niveles de detalle de una malla 3D.

    Usa Quadric Error Metrics (QEM) de Open3D para decimación de malla.
    QEM preserva la forma general mejor que métodos simples de eliminación de vértices.

    Args:
        src:           Archivo de malla 3D de entrada (OBJ, FBX, PLY, GLB, etc.)
        output_dir:    Directorio de salida para los LOD.
        levels:        Lista de MeshLodLevel. Si None usa DEFAULT_MESH_LOD_LEVELS.
        output_format: Extensión de salida (default "glb" para web).
        on_progress:   Callback(percent, label).

    Returns:
        Lista de dicts con {level, label, path, triangle_count, size_bytes}.
    """
    levels = levels or DEFAULT_MESH_LOD_LEVELS
    output_dir.mkdir(parents=True, exist_ok=True)
    fmt = output_format.lstrip(".")

    def _progress(pct: int, label: str) -> None:
        if on_progress:
            on_progress(pct, label)

    # ── Cargar malla original ────────────────────────────────────────
    _progress(5, "Cargando malla 3D...")
    mesh = o3d.io.read_triangle_mesh(str(src), enable_post_processing=True)

    if not mesh.has_vertices():
        raise ValueError(f"No se pudieron leer vértices de {src.name}")

    if not mesh.has_vertex_normals():
        mesh.compute_vertex_normals()

    total_triangles = len(mesh.triangles)
    logger.info(f"Malla cargada: {len(mesh.vertices):,} vértices, {total_triangles:,} triángulos")

    results = []
    total_levels = len(levels)

    for i, lod in enumerate(levels):
        base_pct = 10 + int((i / total_levels) * 80)
        _progress(base_pct, f"Generando LOD {lod.label}...")

        if lod.target_triangles == 0 or total_triangles <= lod.target_triangles:
            # Nivel high o ya tiene menos triángulos: usar malla original
            lod_mesh = mesh
        else:
            # Decimación QEM
            target = lod.target_triangles
            lod_mesh = mesh.simplify_quadric_decimation(target_number_of_triangles=target)
            if not lod_mesh.has_vertex_normals():
                lod_mesh.compute_vertex_normals()

        actual_triangles = len(lod_mesh.triangles)
        out_path = output_dir / f"lod_{lod.label}.{fmt}"
        o3d.io.write_triangle_mesh(str(out_path), lod_mesh, write_ascii=False, compressed=True)

        result = {
            "level":          lod.level,
            "label":          lod.label,
            "path":           out_path,
            "triangle_count": actual_triangles,
            "vertex_count":   len(lod_mesh.vertices),
            "size_bytes":     out_path.stat().st_size,
        }
        results.append(result)

        _progress(base_pct + int(80 / total_levels), f"LOD {lod.label}: {actual_triangles:,} triángulos")
        logger.info("LOD malla generado", extra={"level": lod.label, "triangles": actual_triangles})

    _progress(95, "LOD de malla completados")
    return results
