from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds

logger = logging.getLogger(__name__)

# Algoritmos de remuestreo disponibles
RESAMPLING_METHODS = {
    "nearest":    Resampling.nearest,
    "bilinear":   Resampling.bilinear,
    "cubic":      Resampling.cubic,
    "lanczos":    Resampling.lanczos,
    "average":    Resampling.average,
}


def resize(
    src: Path,
    dst: Path,
    target_width: int | None = None,
    target_height: int | None = None,
    scale_factor: float | None = None,
    resampling: str = "bilinear",
    on_progress: Callable[[int, str], None] | None = None,
) -> dict:
    """
    Redimensiona un archivo GeoTIFF preservando CRS, geotransform y metadatos.

    Acepta target_width/target_height (en píxeles) o scale_factor (0.0-1.0).
    Si solo se especifica uno de width/height, el otro se calcula manteniendo
    la proporción de aspecto original.

    Args:
        src:           GeoTIFF de entrada (puede ser masivo, se procesa en tiles).
        dst:           GeoTIFF de salida.
        target_width:  Ancho destino en píxeles.
        target_height: Alto destino en píxeles.
        scale_factor:  Factor de escala (0.5 = mitad de resolución).
        resampling:    Método de remuestreo (default "bilinear").
        on_progress:   Callback(percent, label).

    Returns:
        Dict con metadatos del archivo generado: width, height, crs, bands.
    """
    resample = RESAMPLING_METHODS.get(resampling, Resampling.bilinear)

    def _progress(pct: int, label: str) -> None:
        if on_progress:
            on_progress(pct, label)

    dst.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(src) as dataset:
        orig_width  = dataset.width
        orig_height = dataset.height
        crs         = dataset.crs
        transform   = dataset.transform
        profile     = dataset.profile.copy()
        band_count  = dataset.count

        # ── Calcular dimensiones de salida ───────────────────────────
        if scale_factor is not None:
            out_width  = max(1, int(orig_width  * scale_factor))
            out_height = max(1, int(orig_height * scale_factor))
        elif target_width and target_height:
            out_width, out_height = target_width, target_height
        elif target_width:
            ratio      = target_width / orig_width
            out_width  = target_width
            out_height = max(1, int(orig_height * ratio))
        elif target_height:
            ratio      = target_height / orig_height
            out_height = target_height
            out_width  = max(1, int(orig_width * ratio))
        else:
            raise ValueError("Especifica target_width, target_height o scale_factor")

        logger.info(
            "Redimensionando GeoTIFF",
            extra={"src": src.name, "from": f"{orig_width}x{orig_height}", "to": f"{out_width}x{out_height}"},
        )

        # Recalcular geotransform para las nuevas dimensiones
        new_transform = from_bounds(
            *dataset.bounds,
            width=out_width,
            height=out_height,
        )

        profile.update(
            width=out_width,
            height=out_height,
            transform=new_transform,
            compress="lzw",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )

        _progress(10, f"Redimensionando a {out_width}x{out_height}px...")

        with rasterio.open(dst, "w", **profile) as out_dataset:
            # Procesar banda por banda para controlar el uso de RAM
            for band_idx in range(1, band_count + 1):
                data = dataset.read(
                    band_idx,
                    out_shape=(1, out_height, out_width),
                    resampling=resample,
                )
                out_dataset.write(data, band_idx)

                pct = int(10 + (band_idx / band_count) * 85)
                _progress(pct, f"Procesando banda {band_idx}/{band_count}...")

            # Copiar metadatos y tags del original
            out_dataset.update_tags(**dataset.tags())

    _progress(98, "GeoTIFF redimensionado")
    logger.info("Resize completado", extra={"dst": dst.name, "size_bytes": dst.stat().st_size})

    return {
        "width":      out_width,
        "height":     out_height,
        "bands":      band_count,
        "crs":        str(crs),
        "size_bytes": dst.stat().st_size,
    }
