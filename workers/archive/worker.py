from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from orchestrator.models.task import TaskOutput, TaskType
from orchestrator.storage import r2_client
from workers.archive.handler import compress, extract
from workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)

_CONTENT_TYPE = {
    ".zip":     "application/zip",
    ".tar":     "application/x-tar",
    ".tar.gz":  "application/gzip",
    ".tgz":     "application/gzip",
    ".tar.bz2": "application/x-bzip2",
    ".tar.xz":  "application/x-xz",
    ".tar.zst": "application/zstd",
    ".7z":      "application/x-7z-compressed",
    ".rar":     "application/vnd.rar",
    ".zst":     "application/zstd",
}


class ArchiveWorker(BaseWorker):
    """
    Worker para tareas archive_compress y archive_extract.

    task.params para archive_compress:
        output_format (str): "zip", "tar.gz", "tar.zst", "7z" (default "zip")
        source_keys   (list[str], opcional): claves R2 adicionales a incluir.
                      Si no se especifica, comprime el archivo fuente.

    task.params para archive_extract:
        (sin parámetros adicionales — extrae todo el archivo fuente)
    """

    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        params = self.task.params
        source = self.task.source
        destination = self.task.destination
        task_type = self.task.task_type

        # ── Etapa 1: Descarga ──────────────────────────────────────────
        if not resume_state or resume_state.get("stage") == "interrupted":
            self.progress.report(5, "download", "Descargando archivo...")

            local_src = self.work_path(Path(source.key).name)

            def _dl_progress(done: int, total: int) -> None:
                pct = int(5 + (done / total) * 20) if total else 5
                self.progress.report(pct, "download", "Descargando archivo...", done, total)

            r2_client.download_file(
                key=source.key,
                local_path=local_src,
                bucket=source.bucket,
                on_progress=_dl_progress,
            )
            self.save_checkpoint({"stage": "downloaded", "local_src": str(local_src)})
        else:
            local_src = Path(resume_state["local_src"])
            logger.info("Retomando desde checkpoint: archivo ya descargado")

        outputs: list[TaskOutput] = []
        base_key = destination.base_key.rstrip("/")

        # ── Etapa 2: Procesamiento ─────────────────────────────────────
        if task_type == TaskType.ARCHIVE_EXTRACT:
            outputs.extend(self._run_extract(local_src, base_key, destination.bucket))

        elif task_type == TaskType.ARCHIVE_COMPRESS:
            outputs.extend(self._run_compress(local_src, base_key, destination.bucket, params))

        self.progress.report(100, "complete", "Procesamiento completado")
        logger.info("ArchiveWorker completado", extra={"task_id": self.task_id})
        return outputs

    # ─────────────────────────────────────────────
    # Helpers privados
    # ─────────────────────────────────────────────

    def _run_extract(self, src: Path, base_key: str, bucket: str) -> list[TaskOutput]:
        self.progress.report(30, "extract", "Extrayendo archivos...")
        extract_dir = self.work_path("extracted")

        def _progress(pct: int, label: str) -> None:
            adjusted = int(30 + pct * 0.45)
            self.progress.report(adjusted, "extract", label)

        result = extract(src, extract_dir, on_progress=_progress)

        # Subir todos los archivos extraídos a R2
        self.progress.report(78, "upload", "Subiendo archivos extraídos a R2...")
        outputs: list[TaskOutput] = []
        extracted_paths = [extract_dir / f for f in result["extracted_files"]]

        for i, file_path in enumerate(extracted_paths):
            if not file_path.exists() or not file_path.is_file():
                continue
            relative = file_path.relative_to(extract_dir)
            dest_key = f"{base_key}/extracted/{relative}"
            ext = file_path.suffix.lower()
            r2_client.upload_file(
                local_path=file_path,
                key=dest_key,
                bucket=bucket,
                content_type=_CONTENT_TYPE.get(ext, "application/octet-stream"),
            )
            outputs.append(TaskOutput(
                type="extracted_file",
                format=ext.lstrip("."),
                key=dest_key,
                size_bytes=file_path.stat().st_size,
            ))
            pct = int(78 + (i / max(len(extracted_paths), 1)) * 20)
            self.progress.report(pct, "upload", f"Subiendo {file_path.name}...")

        logger.info(
            "Extracción y subida completadas",
            extra={"task_id": self.task_id, "files": len(outputs)},
        )
        return outputs

    def _run_compress(self, src: Path, base_key: str, bucket: str, params: dict) -> list[TaskOutput]:
        output_fmt = params.get("output_format", "zip").lstrip(".")

        # Si el archivo fuente es un directorio ya extraído (raro en este contexto),
        # se comprime directamente. Normalmente comprimimos el archivo fuente dentro
        # de un directorio temporal.
        src_dir = self.work_path("to_compress")
        src_dir.mkdir(parents=True, exist_ok=True)

        # Copiar el archivo fuente en el directorio a comprimir
        import shutil
        shutil.copy2(src, src_dir / src.name)

        ext = f".{output_fmt}" if not output_fmt.startswith(".") else output_fmt
        stem = src.stem
        local_dst = self.work_path(f"{stem}{ext}")

        self.progress.report(30, "compress", f"Comprimiendo con formato {output_fmt}...")

        def _progress(pct: int, label: str) -> None:
            adjusted = int(30 + pct * 0.5)
            self.progress.report(adjusted, "compress", label)

        result = compress(src_dir, local_dst, format=output_fmt, on_progress=_progress)

        self.progress.report(82, "upload", "Subiendo archivo comprimido a R2...")

        dest_key = f"{base_key}/{local_dst.name}"

        def _ul_progress(done: int, total: int) -> None:
            pct = int(82 + (done / total) * 16) if total else 82
            self.progress.report(pct, "upload", "Subiendo archivo comprimido a R2...", done, total)

        r2_client.upload_file(
            local_path=local_dst,
            key=dest_key,
            bucket=bucket,
            content_type=_CONTENT_TYPE.get(local_dst.suffix.lower(), "application/octet-stream"),
            on_progress=_ul_progress,
        )

        return [TaskOutput(
            type="compressed",
            format=output_fmt,
            key=dest_key,
            size_bytes=local_dst.stat().st_size,
            metadata={"file_count": result["file_count"]},
        )]
