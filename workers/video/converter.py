from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

import ffmpeg

logger = logging.getLogger(__name__)

# Extensiones de salida soportadas
OUTPUT_FORMATS = {".mp4", ".webm", ".mov", ".mkv", ".avi", ".gif"}

# Codecs por contenedor
_VIDEO_CODEC = {
    ".mp4":  "libx264",
    ".webm": "libvpx-vp9",
    ".mov":  "libx264",
    ".mkv":  "libx264",
    ".avi":  "libx264",
    ".gif":  None,  # Para GIF se usa palettegen/paletteuse
}

_AUDIO_CODEC = {
    ".mp4":  "aac",
    ".webm": "libopus",
    ".mov":  "aac",
    ".mkv":  "aac",
    ".avi":  "aac",
    ".gif":  None,
}


def get_video_metadata(src: Path) -> dict:
    """
    Retorna metadatos del video usando ffprobe.

    Returns:
        Dict con duration_seconds, width, height, fps, has_audio.
    """
    probe = ffmpeg.probe(str(src))
    video_streams = [s for s in probe["streams"] if s["codec_type"] == "video"]
    audio_streams = [s for s in probe["streams"] if s["codec_type"] == "audio"]

    if not video_streams:
        raise ValueError(f"No se encontró stream de video en {src.name}")

    vs = video_streams[0]
    fps_parts = vs.get("r_frame_rate", "30/1").split("/")
    fps = float(fps_parts[0]) / float(fps_parts[1]) if len(fps_parts) == 2 else 30.0

    return {
        "duration_seconds": float(probe["format"].get("duration", 0)),
        "width":            int(vs.get("width", 0)),
        "height":           int(vs.get("height", 0)),
        "fps":              fps,
        "has_audio":        len(audio_streams) > 0,
    }


def transcode(
    src: Path,
    dst: Path,
    crf: int = 23,
    preset: str = "medium",
    max_width: int | None = None,
    max_height: int | None = None,
    fps: float | None = None,
    on_progress: Callable[[int, str], None] | None = None,
) -> dict:
    """
    Transcodifica un video usando FFmpeg.

    Args:
        src:        Video de entrada.
        dst:        Video de salida.
        crf:        Calidad constante (0-51, menor = mejor). Default 23.
        preset:     Preset de velocidad FFmpeg (ultrafast…veryslow). Default "medium".
        max_width:  Ancho máximo (mantiene proporción).
        max_height: Alto máximo (mantiene proporción).
        fps:        FPS de salida. None = mantener original.
        on_progress: Callback(percent, label).

    Returns:
        Dict con size_bytes, duration_seconds, width, height, fps.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst_ext = dst.suffix.lower()

    if dst_ext not in OUTPUT_FORMATS:
        raise ValueError(f"Formato de salida no soportado: {dst_ext}. Disponibles: {OUTPUT_FORMATS}")

    if on_progress:
        on_progress(5, "Analizando video de entrada...")

    meta = get_video_metadata(src)
    duration = meta["duration_seconds"]

    if dst_ext == ".gif":
        _transcode_to_gif(src, dst, max_width, fps, on_progress)
    else:
        _transcode_video(src, dst, dst_ext, crf, preset, max_width, max_height, fps, duration, on_progress)

    if on_progress:
        on_progress(98, "Transcodificación completada")

    out_meta = get_video_metadata(dst)
    result = {
        "size_bytes":        dst.stat().st_size,
        "duration_seconds":  out_meta["duration_seconds"],
        "width":             out_meta["width"],
        "height":            out_meta["height"],
        "fps":               out_meta["fps"],
    }
    logger.info(
        "Video transcodificado",
        extra={"src": src.name, "dst": dst.name, **result},
    )
    return result


def _build_scale_filter(max_width: int | None, max_height: int | None) -> str | None:
    """Construye el filtro scale para FFmpeg respetando la proporción."""
    if max_width and max_height:
        return f"scale='min({max_width},iw)':'min({max_height},ih)':force_original_aspect_ratio=decrease"
    elif max_width:
        return f"scale='min({max_width},iw)':-2"
    elif max_height:
        return f"scale=-2:'min({max_height},ih)'"
    return None


def _transcode_video(
    src: Path,
    dst: Path,
    dst_ext: str,
    crf: int,
    preset: str,
    max_width: int | None,
    max_height: int | None,
    fps: float | None,
    duration: float,
    on_progress: Callable[[int, str], None] | None,
) -> None:
    video_codec = _VIDEO_CODEC[dst_ext]
    audio_codec = _AUDIO_CODEC[dst_ext]

    filters = []
    scale_filter = _build_scale_filter(max_width, max_height)
    if scale_filter:
        filters.append(scale_filter)
    if fps:
        filters.append(f"fps={fps}")

    stream = ffmpeg.input(str(src))
    video = stream.video
    audio = stream.audio

    kwargs: dict = {
        "vcodec":  video_codec,
        "crf":     crf,
        "preset":  preset,
        "movflags": "+faststart",  # Para streaming web
    }

    if filters:
        video = video.filter_multi_output("fps", fps=fps) if fps and not scale_filter else video
        video = ffmpeg.filter(video, "scale", **{}) if False else video
        # Aplicar filtros como vf string
        kwargs["vf"] = ",".join(filters)

    if audio_codec:
        kwargs["acodec"] = audio_codec

    if on_progress:
        on_progress(10, "Transcodificando video...")

    out = ffmpeg.output(stream.video, stream.audio if audio_codec else stream.video, str(dst), **kwargs)

    # Construir comando final con filtros opcionales
    input_stream = ffmpeg.input(str(src))
    output_kwargs: dict = {
        "vcodec":   video_codec,
        "crf":      str(crf),
        "preset":   preset,
        "movflags": "+faststart",
    }
    if audio_codec:
        output_kwargs["acodec"] = audio_codec
    if filters:
        output_kwargs["vf"] = ",".join(filters)

    cmd = ffmpeg.input(str(src)).output(str(dst), **output_kwargs).overwrite_output()
    cmd.run(quiet=True)


def _transcode_to_gif(
    src: Path,
    dst: Path,
    max_width: int | None,
    fps: float | None,
    on_progress: Callable[[int, str], None] | None,
) -> None:
    """Genera GIF de alta calidad usando palettegen + paletteuse."""
    if on_progress:
        on_progress(10, "Generando paleta para GIF...")

    fps_val = fps or 15
    scale_w = max_width or 480

    # Paso 1: generar paleta
    palette_path = dst.with_suffix(".palette.png")
    try:
        (
            ffmpeg
            .input(str(src))
            .filter("fps", fps=fps_val)
            .filter("scale", scale_w, -1, flags="lanczos")
            .filter("palettegen")
            .output(str(palette_path))
            .overwrite_output()
            .run(quiet=True)
        )

        if on_progress:
            on_progress(50, "Aplicando paleta al GIF...")

        # Paso 2: renderizar con paleta
        input_stream  = ffmpeg.input(str(src))
        palette_stream = ffmpeg.input(str(palette_path))
        (
            ffmpeg
            .filter(
                [
                    input_stream.filter("fps", fps=fps_val).filter("scale", scale_w, -1, flags="lanczos"),
                    palette_stream,
                ],
                "paletteuse",
            )
            .output(str(dst))
            .overwrite_output()
            .run(quiet=True)
        )
    finally:
        if palette_path.exists():
            palette_path.unlink()
