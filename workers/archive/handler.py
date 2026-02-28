from __future__ import annotations

import logging
import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

# Extensiones reconocidas por formato
_ZIP_EXTS  = {".zip"}
_TAR_EXTS  = {".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".tar.zst"}
_RAR_EXTS  = {".rar"}
_SEVENZ_EXTS = {".7z"}
_ZST_EXTS  = {".zst"}  # Zstandard sin tar

EXTRACT_FORMATS  = _ZIP_EXTS | _TAR_EXTS | _RAR_EXTS | _SEVENZ_EXTS | _ZST_EXTS
COMPRESS_FORMATS = {".zip", ".tar.gz", ".tar.zst", ".7z"}


def extract(
    src: Path,
    dst_dir: Path,
    on_progress: Callable[[int, str], None] | None = None,
) -> dict:
    """
    Extrae un archivo comprimido en dst_dir.

    Soporta: ZIP, TAR (gz/bz2/xz/zst), RAR, 7ZIP, .zst (solo archivo).

    Returns:
        Dict con extracted_files (lista de rutas relativas), total_size_bytes.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    suffix = _detect_suffix(src)

    if on_progress:
        on_progress(5, f"Extrayendo {src.name}...")

    if suffix in _ZIP_EXTS:
        files = _extract_zip(src, dst_dir, on_progress)
    elif suffix in _TAR_EXTS:
        files = _extract_tar(src, dst_dir, on_progress)
    elif suffix in _RAR_EXTS:
        files = _extract_rar(src, dst_dir, on_progress)
    elif suffix in _SEVENZ_EXTS:
        files = _extract_7z(src, dst_dir, on_progress)
    elif suffix in _ZST_EXTS:
        files = _extract_zst(src, dst_dir, on_progress)
    else:
        raise ValueError(f"Formato no soportado para extracción: {suffix}")

    total_size = sum(
        (dst_dir / f).stat().st_size for f in files if (dst_dir / f).exists()
    )

    if on_progress:
        on_progress(98, f"{len(files)} archivos extraídos")

    logger.info("Extracción completada", extra={"src": src.name, "files": len(files)})
    return {"extracted_files": [str(f) for f in files], "total_size_bytes": total_size}


def compress(
    src_dir: Path,
    dst: Path,
    format: str = "zip",
    on_progress: Callable[[int, str], None] | None = None,
) -> dict:
    """
    Comprime el contenido de src_dir en dst.

    Args:
        src_dir: Directorio fuente.
        dst:     Archivo de salida (.zip, .tar.gz, .tar.zst, .7z).
        format:  "zip", "tar.gz", "tar.zst", "7z".
        on_progress: Callback(percent, label).

    Returns:
        Dict con size_bytes, file_count.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    all_files = [f for f in src_dir.rglob("*") if f.is_file()]

    if on_progress:
        on_progress(5, f"Comprimiendo {len(all_files)} archivos...")

    if format == "zip":
        _compress_zip(src_dir, dst, all_files, on_progress)
    elif format in ("tar.gz", "tgz"):
        _compress_tar(src_dir, dst, all_files, "gz", on_progress)
    elif format in ("tar.zst", "tar.zstd"):
        _compress_tar_zst(src_dir, dst, all_files, on_progress)
    elif format == "7z":
        _compress_7z(src_dir, dst, all_files, on_progress)
    else:
        raise ValueError(f"Formato de compresión no soportado: {format}")

    if on_progress:
        on_progress(98, "Compresión completada")

    result = {"size_bytes": dst.stat().st_size, "file_count": len(all_files)}
    logger.info("Compresión completada", extra={"dst": dst.name, **result})
    return result


# ─────────────────────────────────────────────────────────
# Extracción
# ─────────────────────────────────────────────────────────

def _detect_suffix(src: Path) -> str:
    """Detecta el sufijo completo del archivo (.tar.gz, .tar.zst, etc.)."""
    name = src.name.lower()
    for ext in sorted(_TAR_EXTS, key=len, reverse=True):
        if name.endswith(ext):
            return ext
    return src.suffix.lower()


def _assert_safe_path(dst_dir: Path, member_name: str) -> None:
    """
    Verifica que la ruta del miembro no escape del directorio destino.
    Protege contra ataques de path traversal ('../../etc/passwd', etc.)
    en archivos comprimidos de fuentes no confiables.
    Lanza ValueError si la ruta es insegura.
    """
    target = (dst_dir / member_name).resolve()
    if not target.is_relative_to(dst_dir.resolve()):
        raise ValueError(
            f"Ruta insegura detectada en archivo comprimido: {member_name!r}. "
            "El archivo podría ser malicioso."
        )


def _extract_zip(src: Path, dst_dir: Path, on_progress: Callable | None) -> list[Path]:
    files: list[Path] = []
    with zipfile.ZipFile(src, "r") as zf:
        members = zf.namelist()
        total = len(members)
        for i, member in enumerate(members):
            _assert_safe_path(dst_dir, member)
            zf.extract(member, dst_dir)
            files.append(Path(member))
            if on_progress and total > 0:
                on_progress(int(5 + (i / total) * 90), f"Extrayendo {member}...")
    return files


def _extract_tar(src: Path, dst_dir: Path, on_progress: Callable | None) -> list[Path]:
    files: list[Path] = []
    with tarfile.open(src, "r:*") as tf:
        members = tf.getmembers()
        total = len(members)
        for i, member in enumerate(members):
            tf.extract(member, dst_dir, filter="data")
            files.append(Path(member.name))
            if on_progress and total > 0:
                on_progress(int(5 + (i / total) * 90), f"Extrayendo {member.name}...")
    return files


def _extract_rar(src: Path, dst_dir: Path, on_progress: Callable | None) -> list[Path]:
    # rarfile delega la descompresión al binario 'unrar' (non-free) o 'bsdtar'.
    # Asegúrate de que esté instalado en la imagen Docker:
    #   apt-get install -y unrar-free   (alternativa libre, soporta RAR4 y RAR5)
    import rarfile
    files: list[Path] = []
    with rarfile.RarFile(str(src)) as rf:
        members = rf.infolist()
        total = len(members)
        for i, member in enumerate(members):
            _assert_safe_path(dst_dir, member.filename)
            rf.extract(member, str(dst_dir))
            files.append(Path(member.filename))
            if on_progress and total > 0:
                on_progress(int(5 + (i / total) * 90), f"Extrayendo {member.filename}...")
    return files


def _extract_7z(src: Path, dst_dir: Path, on_progress: Callable | None) -> list[Path]:
    import py7zr
    files: list[Path] = []
    with py7zr.SevenZipFile(str(src), mode="r") as zf:
        names = zf.getnames()
        total = len(names)
        if on_progress:
            on_progress(10, f"Extrayendo {total} archivos 7-Zip...")
        zf.extractall(path=str(dst_dir))
        files = [Path(n) for n in names]
        if on_progress:
            on_progress(90, "Archivos 7-Zip extraídos")
    return files


def _extract_zst(src: Path, dst_dir: Path, on_progress: Callable | None) -> list[Path]:
    """Descomprime un archivo .zst (archivo único sin tar)."""
    import zstandard as zstd
    out_name = src.stem  # e.g. "archivo.tar" si era .tar.zst (pero eso va por _extract_tar)
    out_path = dst_dir / out_name
    if on_progress:
        on_progress(10, "Descomprimiendo Zstandard...")
    with open(src, "rb") as f_in, open(out_path, "wb") as f_out:
        dctx = zstd.ZstdDecompressor()
        dctx.copy_stream(f_in, f_out)
    if on_progress:
        on_progress(90, "Archivo Zstandard descomprimido")
    return [Path(out_name)]


# ─────────────────────────────────────────────────────────
# Compresión
# ─────────────────────────────────────────────────────────

def _compress_zip(
    src_dir: Path,
    dst: Path,
    all_files: list[Path],
    on_progress: Callable | None,
) -> None:
    total = len(all_files)
    with zipfile.ZipFile(dst, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for i, file_path in enumerate(all_files):
            arcname = file_path.relative_to(src_dir)
            zf.write(file_path, arcname)
            if on_progress and total > 0:
                on_progress(int(5 + (i / total) * 90), f"Comprimiendo {arcname}...")


def _compress_tar(
    src_dir: Path,
    dst: Path,
    all_files: list[Path],
    mode: str,
    on_progress: Callable | None,
) -> None:
    total = len(all_files)
    with tarfile.open(dst, f"w:{mode}") as tf:
        for i, file_path in enumerate(all_files):
            arcname = file_path.relative_to(src_dir)
            tf.add(file_path, arcname=str(arcname))
            if on_progress and total > 0:
                on_progress(int(5 + (i / total) * 90), f"Comprimiendo {arcname}...")


def _compress_tar_zst(
    src_dir: Path,
    dst: Path,
    all_files: list[Path],
    on_progress: Callable | None,
) -> None:
    """
    Comprime src_dir en un archivo .tar.zst usando streaming de zstandard.
    Evita cargar el TAR completo en RAM: escribe directamente a disco
    pasando por el compresor como un stream, controlando el uso de memoria.
    """
    import zstandard as zstd

    total = len(all_files)
    cctx = zstd.ZstdCompressor(level=3)

    with open(dst, "wb") as f_out:
        with cctx.stream_writer(f_out, closefd=False) as compressor:
            with tarfile.open(fileobj=compressor, mode="w|") as tf:
                for i, file_path in enumerate(all_files):
                    arcname = file_path.relative_to(src_dir)
                    tf.add(file_path, arcname=str(arcname))
                    if on_progress and total > 0:
                        on_progress(int(5 + (i / total) * 90), f"Comprimiendo {arcname}...")


def _compress_7z(
    src_dir: Path,
    dst: Path,
    all_files: list[Path],
    on_progress: Callable | None,
) -> None:
    import py7zr
    if on_progress:
        on_progress(10, f"Comprimiendo {len(all_files)} archivos con 7-Zip...")
    with py7zr.SevenZipFile(str(dst), mode="w") as zf:
        for file_path in all_files:
            arcname = str(file_path.relative_to(src_dir))
            zf.write(file_path, arcname)
    if on_progress:
        on_progress(90, "Archivo 7-Zip creado")
