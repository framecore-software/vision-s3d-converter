from __future__ import annotations

import json
import logging
from pathlib import Path

import pdal

logger = logging.getLogger(__name__)

# Formatos soportados como entrada
INPUT_FORMATS = {".las", ".laz", ".e57", ".xyz", ".pts", ".ply", ".pcd"}

# Formatos de salida disponibles con el writer PDAL correspondiente
_WRITERS: dict[str, str] = {
    ".las":  "writers.las",
    ".laz":  "writers.las",   # LAZ es LAS comprimido, se activa con compression=true
    ".e57":  "writers.e57",
    ".xyz":  "writers.text",
    ".pts":  "writers.text",
    ".ply":  "writers.ply",
    ".pcd":  "writers.pcd",
}

# Opciones extra por formato de salida
_WRITER_OPTS: dict[str, dict] = {
    ".laz": {"compression": True},
    ".xyz": {"order": "X,Y,Z", "delimiter": " ", "keep_unspecified": False},
    ".pts": {"order": "X,Y,Z,Intensity", "delimiter": " ", "keep_unspecified": False},
}


def convert(src: Path, dst: Path, extra_pipeline_stages: list | None = None) -> Path:
    """
    Convierte un archivo de nube de puntos entre formatos usando PDAL.

    Soporta: LAS ↔ LAZ ↔ E57 ↔ XYZ ↔ PTS ↔ PLY ↔ PCD

    Args:
        src: Archivo de origen.
        dst: Archivo de destino (la extensión determina el formato).
        extra_pipeline_stages: Etapas adicionales de PDAL a insertar entre
                               reader y writer (ej. filtros, reproyección).

    Returns:
        La ruta dst del archivo generado.
    """
    src_ext = src.suffix.lower()
    dst_ext = dst.suffix.lower()

    if dst_ext not in _WRITERS:
        raise ValueError(f"Formato de salida no soportado: {dst_ext}. Disponibles: {list(_WRITERS)}")

    dst.parent.mkdir(parents=True, exist_ok=True)

    writer_type = _WRITERS[dst_ext]
    writer_opts = dict(_WRITER_OPTS.get(dst_ext, {}))
    writer_opts["filename"] = str(dst)

    pipeline_stages: list = [{"type": "readers.auto", "filename": str(src)}]

    if extra_pipeline_stages:
        pipeline_stages.extend(extra_pipeline_stages)

    pipeline_stages.append({"type": writer_type, **writer_opts})

    pipeline = pdal.Pipeline(json.dumps(pipeline_stages))
    pipeline.execute()

    point_count = pipeline.metadata.get("metadata", {}).get("readers.auto", {}).get("count", 0)
    logger.info(
        "Conversión completada",
        extra={"src": src.name, "dst": dst.name, "points": point_count},
    )
    return dst


def get_metadata(src: Path) -> dict:
    """
    Lee metadatos de un archivo de nube de puntos sin cargarlo completamente.
    Retorna: point_count, crs, bbox, format, etc.
    """
    pipeline = pdal.Pipeline(json.dumps([
        {"type": "readers.auto", "filename": str(src)},
    ]))
    pipeline.execute()
    meta = pipeline.metadata.get("metadata", {})
    reader_key = next(iter(meta), None)
    return meta.get(reader_key, {}) if reader_key else {}
