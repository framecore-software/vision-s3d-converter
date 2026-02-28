from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Callable

import laspy
import numpy as np

logger = logging.getLogger(__name__)


def generate_3d_tiles(
    src: Path,
    output_dir: Path,
    tile_size_meters: float = 50.0,
    on_progress: Callable[[int, str], None] | None = None,
) -> dict:
    """
    Genera un tileset 3D Tiles (spec Cesium) desde una nube de puntos.

    Estrategia:
    - Divide el espacio en una cuadrícula de tiles de tile_size_meters x tile_size_meters
    - Cada tile se guarda como PNTS (Point Cloud) o GLB según el tamaño
    - Genera el tileset.json raíz compatible con CesiumJS y otros visores 3D

    Args:
        src:              Archivo LAS/LAZ de entrada.
        output_dir:       Directorio donde se escribe el tileset.
        tile_size_meters: Tamaño de cada tile en metros (default 50m).
        on_progress:      Callback(percent, label) para reportar avance.

    Returns:
        Dict con {tileset_path, tile_count, total_points, bbox}.
    """
    from py3dtiles.convert import convert as py3dtiles_convert
    from py3dtiles.tilers.point.node.node_catalog import NodeCatalog

    def _progress(pct: int, label: str) -> None:
        if on_progress:
            on_progress(pct, label)

    output_dir.mkdir(parents=True, exist_ok=True)

    _progress(5, "Leyendo metadatos de la nube de puntos...")
    las = laspy.read(src)
    total_points = len(las.x)
    logger.info(f"Generando 3D Tiles para {total_points:,} puntos")

    # ── Calcular bounding box ────────────────────────────────────────
    x_min, x_max = float(las.x.min()), float(las.x.max())
    y_min, y_max = float(las.y.min()), float(las.y.max())
    z_min, z_max = float(las.z.min()), float(las.z.max())
    bbox = [x_min, y_min, z_min, x_max, y_max, z_max]

    _progress(10, f"Convirtiendo {total_points:,} puntos a 3D Tiles...")

    # py3dtiles hace la conversión completa: partición en octree + tileset.json
    py3dtiles_convert(
        files=[str(src)],
        outfolder=str(output_dir),
        rgb=True,
        jobs=4,             # Usar 4 procesos internos de py3dtiles
        verbose=False,
    )

    tileset_path = output_dir / "tileset.json"
    tile_count = len(list(output_dir.rglob("*.pnts"))) + len(list(output_dir.rglob("*.glb")))

    _progress(95, f"Tileset generado: {tile_count} tiles")
    logger.info(
        "3D Tiles generado",
        extra={"tiles": tile_count, "output": str(output_dir)},
    )

    return {
        "tileset_path": tileset_path,
        "tile_count":   tile_count,
        "total_points": total_points,
        "bbox":         bbox,
    }


def generate_thumbnail_from_point_cloud(
    src: Path,
    dst: Path,
    width: int = 400,
    height: int = 300,
    on_progress: Callable[[int, str], None] | None = None,
) -> Path:
    """
    Genera una imagen WebP de vista aérea de la nube de puntos usando Open3D.
    Usa el nivel de preview (50K puntos) para que sea rápido.
    """
    import open3d as o3d
    from PIL import Image

    if on_progress:
        on_progress(5, "Generando thumbnail de nube de puntos...")

    las = laspy.read(src)
    xyz = np.vstack([las.x, las.y, las.z]).T.astype(np.float64)

    # Submuestrear si hay demasiados puntos para el thumbnail
    if len(xyz) > 50_000:
        idx = np.random.choice(len(xyz), 50_000, replace=False)
        xyz = xyz[idx]

    pcd = o3d.geometry.PointCloud()
    pcd.points = o3d.utility.Vector3dVector(xyz)

    # Renderizado offscreen (requiere Open3D con soporte de rendering headless)
    vis = o3d.visualization.Visualizer()
    vis.create_window(visible=False, width=width, height=height)
    vis.add_geometry(pcd)
    vis.poll_events()
    vis.update_renderer()

    img_array = np.asarray(vis.capture_screen_float_buffer(do_render=True))
    vis.destroy_window()

    dst.parent.mkdir(parents=True, exist_ok=True)
    img = Image.fromarray((img_array * 255).astype(np.uint8))
    img.save(str(dst), format="WEBP", quality=80)

    if on_progress:
        on_progress(100, "Thumbnail generado")

    return dst
