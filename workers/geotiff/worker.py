from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from orchestrator.models.task import Task, TaskOutput, TaskType
from orchestrator.storage import r2_client
from workers.base_worker import BaseWorker
from workers.geotiff.cog_generator import generate_cog
from workers.geotiff.resizer import resize

logger = logging.getLogger(__name__)

_CONTENT_TYPE = {
    ".tif":  "image/tiff",
    ".tiff": "image/tiff",
}


class GeoTiffWorker(BaseWorker):
    """
    Worker para tareas geotiff_resize y geotiff_cog.

    task.params para geotiff_resize:
        target_width   (int, opcional):   ancho en píxeles
        target_height  (int, opcional):   alto en píxeles
        scale_factor   (float, opcional): factor de escala (0.5 = mitad)
        resampling     (str, opcional):   método de remuestreo (default "bilinear")

    task.params para geotiff_cog:
        profile        (str, opcional): perfil de compresión (default "deflate")
        overview_levels (list, opcional): niveles de overview
    """

    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        params = self.task.params
        source = self.task.source
        destination = self.task.destination
        task_type = self.task.task_type

        # ── Etapa 1: Descarga ──────────────────────────────────────────
        if not resume_state or resume_state.get("stage") == "interrupted":
            self.progress.report(5, "download", "Descargando GeoTIFF...")

            local_src = self.work_path(Path(source.key).name)

            def _dl_progress(done: int, total: int) -> None:
                pct = int(5 + (done / total) * 20) if total else 5
                self.progress.report(pct, "download", "Descargando GeoTIFF...", done, total)

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

        # ── Etapa 2: Procesamiento ─────────────────────────────────────
        stem = Path(source.key).stem
        outputs: list[TaskOutput] = []

        if task_type == TaskType.GEOTIFF_RESIZE:
            local_dst = self.work_path(f"{stem}_resized.tif")

            def _resize_progress(pct: int, label: str) -> None:
                adjusted = int(30 + pct * 0.45)
                self.progress.report(adjusted, "resize", label)

            result = resize(
                src=local_src,
                dst=local_dst,
                target_width=params.get("target_width"),
                target_height=params.get("target_height"),
                scale_factor=params.get("scale_factor"),
                resampling=params.get("resampling", "bilinear"),
                on_progress=_resize_progress,
            )
            output_type = "resized"
            metadata = {
                "width":  result["width"],
                "height": result["height"],
                "bands":  result["bands"],
                "crs":    result["crs"],
            }

        elif task_type == TaskType.GEOTIFF_COG:
            local_dst = self.work_path(f"{stem}_cog.tif")

            def _cog_progress(pct: int, label: str) -> None:
                adjusted = int(30 + pct * 0.45)
                self.progress.report(adjusted, "cog", label)

            result = generate_cog(
                src=local_src,
                dst=local_dst,
                profile=params.get("profile", "deflate"),
                overview_levels=params.get("overview_levels"),
                on_progress=_cog_progress,
            )
            output_type = "cog"
            metadata = {
                "is_valid":      result["is_valid"],
                "overview_count": result["overview_count"],
                "profile":       result["profile"],
            }

        else:
            raise ValueError(f"Tipo de tarea no soportado: {task_type}")

        self.save_checkpoint({
            "stage":     "processed",
            "local_src": str(local_src),
            "local_dst": str(local_dst),
        })

        # ── Etapa 3: Subida a R2 ───────────────────────────────────────
        self.progress.report(80, "upload", "Subiendo resultado a R2...")

        dest_key = destination.base_key.rstrip("/") + "/" + local_dst.name

        def _ul_progress(done: int, total: int) -> None:
            pct = int(80 + (done / total) * 18) if total else 80
            self.progress.report(pct, "upload", "Subiendo resultado a R2...", done, total)

        r2_client.upload_file(
            local_path=local_dst,
            key=dest_key,
            bucket=destination.bucket,
            content_type=_CONTENT_TYPE.get(local_dst.suffix.lower(), "image/tiff"),
            on_progress=_ul_progress,
        )

        outputs.append(TaskOutput(
            type=output_type,
            format="tiff",
            key=dest_key,
            size_bytes=local_dst.stat().st_size,
            metadata=metadata,
        ))

        self.progress.report(100, "complete", "Procesamiento completado")
        logger.info("GeoTiffWorker completado", extra={"task_id": self.task_id, "output_key": dest_key})
        return outputs
