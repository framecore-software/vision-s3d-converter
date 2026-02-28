from __future__ import annotations

import logging
from pathlib import Path

import laspy
import numpy as np

logger = logging.getLogger(__name__)

# Formatos soportados
INPUT_FORMATS  = {".las", ".laz", ".e57", ".xyz", ".pts", ".ply"}
OUTPUT_FORMATS = {".las", ".laz", ".xyz", ".pts", ".ply", ".e57"}


def convert(src: Path, dst: Path) -> Path:
    """
    Convierte un archivo de nube de puntos entre formatos.

    LAS/LAZ ↔ XYZ/PTS (texto) via laspy
    E57 via pye57
    PLY via Open3D

    Returns:
        La ruta dst del archivo generado.
    """
    src_ext = src.suffix.lower()
    dst_ext = dst.suffix.lower()

    if dst_ext not in OUTPUT_FORMATS:
        raise ValueError(f"Formato de salida no soportado: {dst_ext}. Disponibles: {OUTPUT_FORMATS}")

    dst.parent.mkdir(parents=True, exist_ok=True)

    # Cargar la nube de puntos a numpy
    xyz, colors, intensity = _read_to_numpy(src)

    # Escribir al formato destino
    _write_from_numpy(xyz, colors, intensity, dst)

    logger.info(
        "Conversión completada",
        extra={"src": src.name, "dst": dst.name, "points": len(xyz)},
    )
    return dst


def get_metadata(src: Path) -> dict:
    """
    Lee metadatos de un archivo de nube de puntos sin cargarlo completamente.
    """
    src_ext = src.suffix.lower()
    if src_ext in (".las", ".laz"):
        with laspy.open(src) as f:
            return {
                "point_count": f.header.point_count,
                "crs":         str(f.header.parse_crs()) if hasattr(f.header, "parse_crs") else "",
                "format":      src_ext.lstrip("."),
                "x_min": float(f.header.mins[0]) if hasattr(f.header, "mins") else 0,
                "y_min": float(f.header.mins[1]) if hasattr(f.header, "mins") else 0,
                "z_min": float(f.header.mins[2]) if hasattr(f.header, "mins") else 0,
                "x_max": float(f.header.maxs[0]) if hasattr(f.header, "maxs") else 0,
                "y_max": float(f.header.maxs[1]) if hasattr(f.header, "maxs") else 0,
                "z_max": float(f.header.maxs[2]) if hasattr(f.header, "maxs") else 0,
            }
    # Para otros formatos cargamos completo
    xyz, _, _ = _read_to_numpy(src)
    return {
        "point_count": len(xyz),
        "format": src_ext.lstrip("."),
    }


# ─────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────

def _read_to_numpy(src: Path) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None]:
    """Devuelve (xyz, colors_rgb_0_1, intensity_normalized)."""
    ext = src.suffix.lower()

    if ext in (".las", ".laz"):
        las = laspy.read(src)
        xyz = np.vstack([las.x, las.y, las.z]).T.astype(np.float64)
        colors = None
        if hasattr(las, "red"):
            r = np.asarray(las.red,   dtype=np.float32) / 65535.0
            g = np.asarray(las.green, dtype=np.float32) / 65535.0
            b = np.asarray(las.blue,  dtype=np.float32) / 65535.0
            colors = np.column_stack([r, g, b])
        intensity = None
        if hasattr(las, "intensity"):
            intensity = np.asarray(las.intensity, dtype=np.float32) / 65535.0
        return xyz, colors, intensity

    elif ext == ".e57":
        import pye57
        e57 = pye57.E57(str(src))
        data = e57.read_scan(0, intensity=True, colors=True, ignore_missing_fields=True)
        xyz = np.column_stack([data["cartesianX"], data["cartesianY"], data["cartesianZ"]])
        colors = None
        if "colorRed" in data:
            colors = np.column_stack([
                data["colorRed"] / 255.0,
                data["colorGreen"] / 255.0,
                data["colorBlue"] / 255.0,
            ])
        intensity = data.get("intensity")
        return xyz.astype(np.float64), colors, intensity

    elif ext in (".xyz", ".pts"):
        # Formato texto: X Y Z [Intensity] [R G B]
        raw = np.loadtxt(src)
        xyz = raw[:, :3].astype(np.float64)
        colors = raw[:, 3:6].astype(np.float32) / 255.0 if raw.shape[1] >= 6 else None
        return xyz, colors, None

    elif ext == ".ply":
        import open3d as o3d
        pcd = o3d.io.read_point_cloud(str(src))
        xyz = np.asarray(pcd.points)
        colors = np.asarray(pcd.colors) if pcd.has_colors() else None
        return xyz.astype(np.float64), colors, None

    else:
        raise ValueError(f"Formato de entrada no soportado: {ext}")


def _write_from_numpy(
    xyz: np.ndarray,
    colors: np.ndarray | None,
    intensity: np.ndarray | None,
    dst: Path,
) -> None:
    ext = dst.suffix.lower()

    if ext in (".las", ".laz"):
        header = laspy.LasHeader(point_format=2 if colors is not None else 0, version="1.4")
        las = laspy.LasData(header=header)
        las.x = xyz[:, 0]
        las.y = xyz[:, 1]
        las.z = xyz[:, 2]
        if colors is not None:
            las.red   = (colors[:, 0] * 65535).astype(np.uint16)
            las.green = (colors[:, 1] * 65535).astype(np.uint16)
            las.blue  = (colors[:, 2] * 65535).astype(np.uint16)
        if intensity is not None:
            las.intensity = (intensity * 65535).astype(np.uint16)
        las.write(str(dst), do_compress=(ext == ".laz"))

    elif ext == ".e57":
        import pye57
        e57 = pye57.E57(str(dst), mode="w")
        data = {
            "cartesianX": xyz[:, 0],
            "cartesianY": xyz[:, 1],
            "cartesianZ": xyz[:, 2],
        }
        if colors is not None:
            data["colorRed"]   = (colors[:, 0] * 255).astype(np.uint8)
            data["colorGreen"] = (colors[:, 1] * 255).astype(np.uint8)
            data["colorBlue"]  = (colors[:, 2] * 255).astype(np.uint8)
        if intensity is not None:
            data["intensity"] = intensity
        e57.write_scan_raw(data)
        e57.close()

    elif ext in (".xyz", ".pts"):
        if colors is not None:
            rgb_255 = (colors * 255).astype(int)
            rows = np.hstack([xyz, rgb_255])
            header = "X Y Z R G B"
        else:
            rows = xyz
            header = "X Y Z"
        np.savetxt(dst, rows, fmt="%.6f", header=header, comments="")

    elif ext == ".ply":
        import open3d as o3d
        pcd = o3d.geometry.PointCloud()
        pcd.points = o3d.utility.Vector3dVector(xyz)
        if colors is not None:
            pcd.colors = o3d.utility.Vector3dVector(colors)
        o3d.io.write_point_cloud(str(dst), pcd, compressed=True)

    else:
        raise ValueError(f"Formato de salida no soportado: {ext}")
