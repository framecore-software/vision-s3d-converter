from __future__ import annotations

from pathlib import Path

import pyvips

# Mapeo de extensión → formato pyvips
_VIPS_FORMAT: dict[str, str] = {
    ".jpg": "jpeg",
    ".jpeg": "jpeg",
    ".png": "png",
    ".webp": "webp",
    ".tiff": "tiff",
    ".tif": "tiff",
    ".avif": "avif",
}

# Opciones de calidad por formato
_DEFAULT_QUALITY: dict[str, dict] = {
    "jpeg": {"Q": 85, "strip": True},
    "webp": {"Q": 82, "strip": True},
    "avif": {"Q": 70, "strip": True},
    "png": {"compression": 6},
    "tiff": {"compression": "lzw"},
}


def convert_image(
    src: Path,
    dst: Path,
    quality: int | None = None,
    max_width: int | None = None,
    max_height: int | None = None,
) -> Path:
    """
    Convierte y opcionalmente redimensiona una imagen usando pyvips.

    pyvips procesa en streaming por tiles → uso de RAM constante
    independientemente del tamaño del archivo de entrada.

    Args:
        src: Ruta del archivo de origen.
        dst: Ruta del archivo de destino (la extensión determina el formato).
        quality: Calidad 1-100. Si None, usa el valor por defecto del formato.
        max_width: Ancho máximo. Redimensiona manteniendo proporción si se supera.
        max_height: Alto máximo. Redimensiona manteniendo proporción si se supera.

    Returns:
        La ruta dst del archivo generado.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    ext = dst.suffix.lower()
    fmt = _VIPS_FORMAT.get(ext)
    if fmt is None:
        raise ValueError(f"Formato de salida no soportado: {ext}")

    image = pyvips.Image.new_from_file(str(src), access="sequential")

    if max_width or max_height:
        image = _resize_to_fit(image, max_width, max_height)

    save_opts = dict(_DEFAULT_QUALITY.get(fmt, {}))
    if quality is not None:
        if fmt in ("jpeg", "webp", "avif"):
            save_opts["Q"] = quality

    image.write_to_file(str(dst), **save_opts)
    return dst


def generate_thumbnail(
    src: Path,
    dst: Path,
    width: int = 400,
    height: int = 300,
) -> Path:
    """
    Genera un thumbnail recortado al centro usando pyvips thumbnail_image.
    Más rápido que cargar la imagen completa para archivos grandes.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)

    # pyvips.thumbnail genera el thumbnail directamente desde el archivo
    # sin cargar toda la imagen en memoria
    image = pyvips.Image.thumbnail(
        str(src),
        width,
        height=height,
        crop=pyvips.Interesting.CENTRE,
    )

    ext = dst.suffix.lower()
    fmt = _VIPS_FORMAT.get(ext, "webp")
    save_opts = dict(_DEFAULT_QUALITY.get(fmt, {}))
    image.write_to_file(str(dst), **save_opts)
    return dst


def _resize_to_fit(
    image: pyvips.Image,
    max_width: int | None,
    max_height: int | None,
) -> pyvips.Image:
    """Redimensiona manteniendo proporción sin superar max_width x max_height."""
    w, h = image.width, image.height
    scale = 1.0

    if max_width and w > max_width:
        scale = min(scale, max_width / w)
    if max_height and h > max_height:
        scale = min(scale, max_height / h)

    if scale < 1.0:
        image = image.resize(scale)

    return image
