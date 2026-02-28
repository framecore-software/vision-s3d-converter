from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from orchestrator.models.task import Task, TaskOutput
from orchestrator.storage import r2_client
from workers.base_worker import BaseWorker
from workers.image.converter import convert_image, generate_thumbnail

logger = logging.getLogger(__name__)

# Tipos MIME por extensión de salida
_CONTENT_TYPE: dict[str, str] = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    ".avif": "image/avif",
}


class ImageWorker(BaseWorker):
    """
    Worker para tareas image_convert e image_thumbnail.

    Parámetros esperados en task.params:
        output_format (str): extensión de salida, ej. "webp"
        quality (int, opcional): calidad 1-100
        max_width (int, opcional): ancho máximo en píxeles
        max_height (int, opcional): alto máximo en píxeles
        thumbnail_width (int, opcional): ancho del thumbnail (default 400)
        thumbnail_height (int, opcional): alto del thumbnail (default 300)
    """

    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        params = self.task.params
        source = self.task.source
        destination = self.task.destination

        # ── Etapa 1: Descarga ──────────────────────────────────────────
        if not resume_state or resume_state.get("stage") == "interrupted":
            self.progress.report(5, "download", "Descargando archivo...")

            local_src = self.work_path(Path(source.key).name)

            def on_download(done: int, total: int) -> None:
                pct = int(5 + (done / total) * 20) if total else 5
                self.progress.report(pct, "download", "Descargando archivo...", done, total)

            r2_client.download_file(
                key=source.key,
                local_path=local_src,
                bucket=source.bucket,
                on_progress=on_download,
            )
            self.save_checkpoint({"stage": "downloaded", "local_src": str(local_src)})
        else:
            local_src = Path(resume_state["local_src"])
            logger.info("Retomando desde checkpoint: archivo ya descargado")

        # ── Etapa 2: Procesamiento ─────────────────────────────────────
        self.progress.report(30, "processing", "Procesando imagen...")

        output_format = params.get("output_format", "webp").lstrip(".")
        quality = params.get("quality")
        max_width = params.get("max_width")
        max_height = params.get("max_height")
        task_type = self.task.task_type

        outputs: list[TaskOutput] = []

        if task_type == "image_thumbnail":
            thumb_width = params.get("thumbnail_width", 400)
            thumb_height = params.get("thumbnail_height", 300)
            output_filename = f"thumbnail.{output_format}"
            local_dst = self.work_path(output_filename)

            generate_thumbnail(local_src, local_dst, width=thumb_width, height=thumb_height)
            self.progress.report(70, "processing", "Thumbnail generado")
            output_type = "thumbnail"
        else:
            stem = Path(source.key).stem
            output_filename = f"{stem}.{output_format}"
            local_dst = self.work_path(output_filename)

            convert_image(local_src, local_dst, quality=quality, max_width=max_width, max_height=max_height)
            self.progress.report(70, "processing", "Imagen convertida")
            output_type = "converted"

        self.save_checkpoint({"stage": "processed", "local_src": str(local_src), "local_dst": str(local_dst)})

        # ── Etapa 3: Subida a R2 ───────────────────────────────────────
        self.progress.report(75, "upload", "Subiendo resultado a R2...")

        dest_key = destination.base_key.rstrip("/") + "/" + output_filename
        ext = local_dst.suffix.lower()
        content_type = _CONTENT_TYPE.get(ext, "application/octet-stream")

        def on_upload(done: int, total: int) -> None:
            pct = int(75 + (done / total) * 20) if total else 75
            self.progress.report(pct, "upload", "Subiendo resultado a R2...", done, total)

        r2_client.upload_file(
            local_path=local_dst,
            key=dest_key,
            bucket=destination.bucket,
            content_type=content_type,
            on_progress=on_upload,
        )

        outputs.append(TaskOutput(
            type=output_type,
            format=output_format,
            key=dest_key,
            size_bytes=local_dst.stat().st_size,
        ))

        self.progress.report(100, "complete", "Procesamiento completado")
        logger.info("ImageWorker completado", extra={"task_id": self.task_id, "output_key": dest_key})
        return outputs
