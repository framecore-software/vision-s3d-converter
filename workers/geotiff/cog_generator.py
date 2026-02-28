from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logger = logging.getLogger(__name__)


def generate_cog(
    src: Path,
    dst: Path,
    profile: str = "deflate",
    overview_levels: list[int] | None = None,
    on_progress: Callable[[int, str], None] | None = None,
) -> dict:
    """
    Convierte un GeoTIFF a Cloud Optimized GeoTIFF (COG).

    Un COG tiene las tiles internas y los overviews (pirámides) organizados
    para que visores web puedan leer solo la región y resolución necesaria,
    sin descargar el archivo completo.

    Args:
        src:             GeoTIFF de entrada.
        dst:             COG de salida.
        profile:         Perfil de compresión: "deflate" (default), "lzw", "webp", "zstd".
        overview_levels: Niveles de overview [2, 4, 8, 16, 32, 64].
                         Si None usa los niveles estándar.
        on_progress:     Callback(percent, label).

    Returns:
        Dict con is_valid, size_bytes, overview_count.
    """
    from rio_cogeo.cogeo import cog_validate

    def _progress(pct: int, label: str) -> None:
        if on_progress:
            on_progress(pct, label)

    dst.parent.mkdir(parents=True, exist_ok=True)

    if overview_levels is None:
        # Calcular niveles apropiados según las dimensiones reales de la imagen.
        # No tiene sentido generar un overview 64x si la imagen tiene 256px de lado.
        import rasterio
        with rasterio.open(src) as ds:
            min_dim = min(ds.width, ds.height)
        # Incluir solo niveles donde la dimensión resultante >= 256px
        overview_levels = [2 ** i for i in range(1, 7) if min_dim // (2 ** i) >= 256]
        if not overview_levels:
            overview_levels = [2]  # mínimo un overview

    _progress(5, "Generando COG con pirámides...")
    logger.info(
        "Generando COG",
        extra={"src": src.name, "profile": profile, "overview_levels": overview_levels},
    )

    cog_profile = cog_profiles.get(profile)
    if cog_profile is None:
        raise ValueError(f"Perfil COG no válido: {profile}. Opciones: deflate, lzw, webp, zstd")

    cog_translate(
        input=str(src),
        output=str(dst),
        profile=cog_profile,
        overview_level=len(overview_levels),
        overview_resampling="average",
        use_cog_driver=True,
        quiet=True,
    )

    _progress(85, "Validando COG...")

    # Validar que el archivo generado sea un COG correcto
    is_valid, errors, warnings = cog_validate(str(dst))
    if not is_valid:
        logger.warning("COG generado con advertencias", extra={"errors": errors, "warnings": warnings})

    _progress(98, "COG generado")
    result = {
        "is_valid":       is_valid,
        "size_bytes":     dst.stat().st_size,
        "overview_count": len(overview_levels),
        "profile":        profile,
    }
    logger.info("COG completado", extra=result)
    return result
