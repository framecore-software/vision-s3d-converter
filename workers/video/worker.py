from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from orchestrator.models.task import TaskOutput, TaskType
from orchestrator.storage import r2_client
from workers.base_worker import BaseWorker
from workers.video.converter import transcode

logger = logging.getLogger(__name__)

_CONTENT_TYPE = {
    ".mp4":  "video/mp4",
    ".webm": "video/webm",
    ".mov":  "video/quicktime",
    ".mkv":  "video/x-matroska",
    ".avi":  "video/x-msvideo",
    ".gif":  "image/gif",
}


class VideoWorker(BaseWorker):
    """
    Worker para tareas video_transcode.

    task.params:
        output_format (str):          extensión de salida, ej. "mp4", "webm", "gif"
        crf           (int, opcional): calidad constante (0-51, default 23)
        preset        (str, opcional): preset FFmpeg (default "medium")
        max_width     (int, opcional): ancho máximo en píxeles
        max_height    (int, opcional): alto máximo en píxeles
        fps           (float, opcional): FPS de salida
    """

    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        params = self.task.params
        source = self.task.source
        destination = self.task.destination

        # ── Etapa 1: Descarga ──────────────────────────────────────────
        if not resume_state or resume_state.get("stage") == "interrupted":
            self.progress.report(5, "download", "Descargando video...")

            local_src = self.work_path(Path(source.key).name)

            def _dl_progress(done: int, total: int) -> None:
                pct = int(5 + (done / total) * 20) if total else 5
                self.progress.report(pct, "download", "Descargando video...", done, total)

            r2_client.download_file(
                key=source.key,
                local_path=local_src,
                bucket=source.bucket,
                on_progress=_dl_progress,
            )
            self.save_checkpoint({"stage": "downloaded", "local_src": str(local_src)})
        else:
            local_src = Path(resume_state["local_src"])
            logger.debug("Retomando desde checkpoint: video ya descargado")

        # ── Etapa 2: Transcodificación ─────────────────────────────────
        output_fmt = params.get("output_format", "mp4").lstrip(".")
        stem = Path(source.key).stem
        local_dst = self.work_path(f"{stem}.{output_fmt}")

        def _transcode_progress(pct: int, label: str) -> None:
            adjusted = int(30 + pct * 0.5)
            self.progress.report(adjusted, "transcode", label)

        result = transcode(
            src=local_src,
            dst=local_dst,
            crf=params.get("crf", 23),
            preset=params.get("preset", "medium"),
            max_width=params.get("max_width"),
            max_height=params.get("max_height"),
            fps=params.get("fps"),
            on_progress=_transcode_progress,
        )

        self.save_checkpoint({
            "stage":     "transcoded",
            "local_src": str(local_src),
            "local_dst": str(local_dst),
        })

        # ── Etapa 3: Subida a R2 ───────────────────────────────────────
        self.progress.report(82, "upload", "Subiendo video a R2...")

        dest_key = destination.base_key.rstrip("/") + "/" + local_dst.name
        ext = local_dst.suffix.lower()

        def _ul_progress(done: int, total: int) -> None:
            pct = int(82 + (done / total) * 16) if total else 82
            self.progress.report(pct, "upload", "Subiendo video a R2...", done, total)

        r2_client.upload_file(
            local_path=local_dst,
            key=dest_key,
            bucket=destination.bucket,
            content_type=_CONTENT_TYPE.get(ext, "video/mp4"),
            on_progress=_ul_progress,
        )

        outputs = [TaskOutput(
            type="transcoded",
            format=output_fmt,
            key=dest_key,
            size_bytes=local_dst.stat().st_size,
            metadata={
                "duration_seconds": result["duration_seconds"],
                "width":            result["width"],
                "height":           result["height"],
                "fps":              result["fps"],
            },
        )]

        self.progress.report(100, "complete", "Procesamiento completado")
        logger.info("VideoWorker completado", extra={"task_id": self.task_id, "output_key": dest_key})
        return outputs
