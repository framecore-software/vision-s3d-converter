from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from orchestrator.models.task import Task, TaskOutput, TaskType
from orchestrator.storage import r2_client
from workers.base_worker import BaseWorker
from workers.point_cloud.format_converter import convert, get_metadata
from workers.point_cloud.lod_generator import DEFAULT_LOD_LEVELS, LodLevel, generate_lod
from workers.point_cloud.tile_generator import generate_3d_tiles, generate_thumbnail_from_point_cloud

logger = logging.getLogger(__name__)

_CONTENT_TYPE = {
    ".las":  "application/octet-stream",
    ".laz":  "application/octet-stream",
    ".e57":  "application/octet-stream",
    ".xyz":  "text/plain",
    ".pts":  "text/plain",
    ".ply":  "application/octet-stream",
    ".glb":  "model/gltf-binary",
    ".webp": "image/webp",
    ".json": "application/json",
}


class PointCloudWorker(BaseWorker):
    """
    Worker para tareas point_cloud_lod y point_cloud_convert.

    task.params para point_cloud_lod:
        lod_levels        (list, opcional): lista de {level, label, max_points}
        output_format     (str, opcional):  formato de salida de los LOD ("laz" default)
        generate_tiles    (bool, opcional): generar 3D Tiles (default True)
        tile_size_meters  (float, opcional): tamaño de tile en metros (default 50)
        generate_thumbnails (bool, opcional): generar thumbnail WebP (default True)

    task.params para point_cloud_convert:
        output_format (str): extensión destino (e.g. "e57", "xyz", "ply")
    """

    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        params = self.task.params
        source = self.task.source
        destination = self.task.destination
        task_type = self.task.task_type

        # ── Etapa 1: Descarga ──────────────────────────────────────────
        stage_key = "downloaded"
        if not resume_state or resume_state.get("stage") not in (
            "downloaded", "lod_done", "tiles_done"
        ):
            self.progress.report(2, "download", "Descargando archivo...")
            local_src = self.work_path(Path(source.key).name)

            def _dl_progress(done: int, total: int) -> None:
                pct = int(2 + (done / total) * 18) if total else 2
                self.progress.report(pct, "download", "Descargando archivo...", done, total)

            r2_client.download_file(
                key=source.key,
                local_path=local_src,
                bucket=source.bucket,
                on_progress=_dl_progress,
            )
            self.save_checkpoint({"stage": stage_key, "local_src": str(local_src)})
        else:
            local_src = Path(resume_state["local_src"])
            logger.debug("Retomando: archivo ya descargado")

        outputs: list[TaskOutput] = []
        base_key = destination.base_key.rstrip("/")

        # ── Etapa 2: Procesamiento ─────────────────────────────────────
        if task_type == TaskType.POINT_CLOUD_CONVERT:
            outputs.extend(self._run_convert(local_src, base_key, destination.bucket, params))

        elif task_type == TaskType.POINT_CLOUD_LOD:
            already_lod = resume_state.get("stage") in ("lod_done", "tiles_done") if resume_state else False
            lod_outputs = resume_state.get("lod_outputs", []) if resume_state else []

            if not already_lod:
                lod_outputs = self._run_lod(local_src, base_key, destination.bucket, params)
                self.save_checkpoint({
                    "stage": "lod_done",
                    "local_src": str(local_src),
                    "lod_outputs": [o.model_dump() for o in lod_outputs],
                })
            else:
                lod_outputs = [TaskOutput(**o) for o in lod_outputs]

            outputs.extend(lod_outputs)

            # Tiles 3D (opcional)
            # Se generan si: la tarea lo pide Y aún no están hechos (stage != "tiles_done")
            tiles_already_done = resume_state is not None and resume_state.get("stage") == "tiles_done"
            if params.get("generate_tiles", True) and not tiles_already_done:
                tile_outputs = self._run_tiles(local_src, base_key, destination.bucket, params)
                outputs.extend(tile_outputs)
                self.save_checkpoint({
                    "stage": "tiles_done",
                    "local_src": str(local_src),
                    "lod_outputs": [o.model_dump() for o in lod_outputs],
                })

            # Thumbnail (opcional)
            if params.get("generate_thumbnails", True):
                thumb_output, thumb_error = self._run_thumbnail(local_src, base_key, destination.bucket)
                if thumb_output:
                    outputs.append(thumb_output)
                elif thumb_error:
                    # Registrar en los outputs que el thumbnail falló, para que
                    # Laravel pueda informarlo al usuario sin bloquear la tarea.
                    outputs.append(TaskOutput(
                        type="thumbnail_error",
                        format="",
                        key="",
                        metadata={"error": thumb_error},
                    ))

        self.progress.report(100, "complete", "Procesamiento completado")
        return outputs

    # ─────────────────────────────────────────────
    # Helpers privados
    # ─────────────────────────────────────────────

    def _run_convert(self, src: Path, base_key: str, bucket: str, params: dict) -> list[TaskOutput]:
        self.progress.report(25, "converting", "Convirtiendo formato...")
        output_fmt = params.get("output_format", "laz").lstrip(".")
        dst = self.work_path(f"{src.stem}.{output_fmt}")
        convert(src, dst)
        self.progress.report(70, "uploading", "Subiendo resultado...")
        dest_key = f"{base_key}/{dst.name}"
        r2_client.upload_file(dst, dest_key, bucket, _CONTENT_TYPE.get(dst.suffix))
        return [TaskOutput(type="converted", format=output_fmt, key=dest_key, size_bytes=dst.stat().st_size)]

    def _run_lod(self, src: Path, base_key: str, bucket: str, params: dict) -> list[TaskOutput]:
        raw_levels = params.get("lod_levels")
        levels = [LodLevel(**l) for l in raw_levels] if raw_levels else DEFAULT_LOD_LEVELS
        output_fmt = params.get("output_format", "laz")

        lod_dir = self.work_path("lod")

        def _lod_progress(pct: int, label: str) -> None:
            adjusted = int(20 + pct * 0.5)
            self.progress.report(adjusted, "lod", label)

        lod_results = generate_lod(src, lod_dir, levels, output_fmt, _lod_progress)

        outputs = []
        for result in lod_results:
            file_path: Path = result["path"]
            dest_key = f"{base_key}/lod/{file_path.name}"
            self.progress.report(70, "uploading", f"Subiendo LOD {result['label']}...")
            r2_client.upload_file(file_path, dest_key, bucket, _CONTENT_TYPE.get(file_path.suffix))
            outputs.append(TaskOutput(
                type=f"lod_{result['label']}",
                format=output_fmt,
                key=dest_key,
                size_bytes=result["size_bytes"],
                metadata={"point_count": result["point_count"], "level": result["level"]},
            ))
        return outputs

    def _run_tiles(self, src: Path, base_key: str, bucket: str, params: dict) -> list[TaskOutput]:
        self.progress.report(75, "tiling", "Generando 3D Tiles...")
        tiles_dir = self.work_path("tiles")
        tile_size = params.get("tile_size_meters", 50.0)

        def _tile_progress(pct: int, label: str) -> None:
            adjusted = int(75 + pct * 0.15)
            self.progress.report(adjusted, "tiling", label)

        result = generate_3d_tiles(src, tiles_dir, tile_size, _tile_progress)
        tileset_key = f"{base_key}/tiles/tileset.json"

        outputs = []
        # Subir todos los archivos del tileset
        for tile_file in tiles_dir.rglob("*"):
            if tile_file.is_file():
                relative = tile_file.relative_to(tiles_dir)
                dest_key = f"{base_key}/tiles/{relative}"
                r2_client.upload_file(tile_file, dest_key, bucket, _CONTENT_TYPE.get(tile_file.suffix, "application/octet-stream"))

        outputs.append(TaskOutput(
            type="3d_tiles",
            format="3dtiles",
            key=tileset_key,
            size_bytes=0,
            metadata={"tile_count": result["tile_count"], "total_points": result["total_points"], "bbox": result["bbox"]},
        ))
        return outputs

    def _run_thumbnail(self, src: Path, base_key: str, bucket: str) -> tuple[TaskOutput | None, str | None]:
        """
        Genera y sube el thumbnail. Retorna (output, None) en éxito,
        (None, error_str) en fallo. Nunca propaga excepciones porque el
        thumbnail es opcional y no debe bloquear el resultado principal.
        """
        try:
            self.progress.report(92, "thumbnail", "Generando thumbnail...")
            thumb_path = self.work_path("thumbnail.webp")
            generate_thumbnail_from_point_cloud(src, thumb_path)
            dest_key = f"{base_key}/thumbnail.webp"
            r2_client.upload_file(thumb_path, dest_key, bucket, "image/webp")
            return TaskOutput(type="thumbnail", format="webp", key=dest_key, size_bytes=thumb_path.stat().st_size), None
        except Exception as exc:
            error_msg = str(exc)
            logger.warning("No se pudo generar thumbnail: %s", error_msg, extra={"task_id": self.task_id})
            return None, error_msg
