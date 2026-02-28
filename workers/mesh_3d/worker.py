from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from orchestrator.models.task import Task, TaskOutput, TaskType
from orchestrator.storage import r2_client
from workers.base_worker import BaseWorker
from workers.mesh_3d.converter import convert_mesh
from workers.mesh_3d.lod_generator import DEFAULT_MESH_LOD_LEVELS, MeshLodLevel, generate_mesh_lod

logger = logging.getLogger(__name__)

_CONTENT_TYPE = {
    ".glb":  "model/gltf-binary",
    ".gltf": "model/gltf+json",
    ".obj":  "model/obj",
    ".ply":  "application/octet-stream",
    ".stl":  "model/stl",
}


class Mesh3DWorker(BaseWorker):
    """
    Worker para tareas mesh_3d_lod y mesh_3d_convert.

    task.params para mesh_3d_lod:
        lod_levels    (list, opcional): [{level, label, target_triangles}]
        output_format (str, opcional):  "glb" (default)

    task.params para mesh_3d_convert:
        output_format (str): extensión destino ("glb", "ply", "obj", etc.)
    """

    def process(self, resume_state: dict[str, Any] | None = None) -> list[TaskOutput]:
        params = self.task.params
        source = self.task.source
        destination = self.task.destination
        task_type = self.task.task_type

        # ── Descarga ───────────────────────────────────────────────────
        if not resume_state or resume_state.get("stage") not in ("downloaded", "lod_done"):
            self.progress.report(5, "download", "Descargando malla 3D...")
            local_src = self.work_path(Path(source.key).name)

            def _dl(done: int, total: int) -> None:
                pct = int(5 + (done / total) * 15) if total else 5
                self.progress.report(pct, "download", "Descargando...", done, total)

            r2_client.download_file(source.key, local_src, source.bucket, _dl)
            self.save_checkpoint({"stage": "downloaded", "local_src": str(local_src)})
        else:
            local_src = Path(resume_state["local_src"])

        base_key = destination.base_key.rstrip("/")
        outputs: list[TaskOutput] = []

        if task_type == TaskType.MESH_3D_CONVERT:
            outputs.extend(self._run_convert(local_src, base_key, destination.bucket, params))

        elif task_type == TaskType.MESH_3D_LOD:
            already_done = resume_state.get("stage") == "lod_done" if resume_state else False
            lod_out = resume_state.get("lod_outputs", []) if resume_state else []

            if not already_done:
                lod_out = self._run_lod(local_src, base_key, destination.bucket, params)
                self.save_checkpoint({
                    "stage": "lod_done",
                    "local_src": str(local_src),
                    "lod_outputs": [o.model_dump() for o in lod_out],
                })
            else:
                lod_out = [TaskOutput(**o) for o in lod_out]

            outputs.extend(lod_out)

        self.progress.report(100, "complete", "Procesamiento completado")
        return outputs

    def _run_convert(self, src: Path, base_key: str, bucket: str, params: dict) -> list[TaskOutput]:
        self.progress.report(25, "converting", "Convirtiendo malla...")
        fmt = params.get("output_format", "glb").lstrip(".")
        dst = self.work_path(f"{src.stem}.{fmt}")
        convert_mesh(src, dst)
        self.progress.report(75, "uploading", "Subiendo resultado...")
        dest_key = f"{base_key}/{dst.name}"
        r2_client.upload_file(dst, dest_key, bucket, _CONTENT_TYPE.get(dst.suffix))
        return [TaskOutput(type="converted", format=fmt, key=dest_key, size_bytes=dst.stat().st_size)]

    def _run_lod(self, src: Path, base_key: str, bucket: str, params: dict) -> list[TaskOutput]:
        raw_levels = params.get("lod_levels")
        levels = [MeshLodLevel(**l) for l in raw_levels] if raw_levels else DEFAULT_MESH_LOD_LEVELS
        fmt = params.get("output_format", "glb")
        lod_dir = self.work_path("lod")

        def _lod_progress(pct: int, label: str) -> None:
            self.progress.report(int(20 + pct * 0.6), "lod", label)

        lod_results = generate_mesh_lod(src, lod_dir, levels, fmt, _lod_progress)

        outputs = []
        for result in lod_results:
            file_path: Path = result["path"]
            dest_key = f"{base_key}/lod/{file_path.name}"
            self.progress.report(82, "uploading", f"Subiendo LOD {result['label']}...")
            r2_client.upload_file(file_path, dest_key, bucket, _CONTENT_TYPE.get(file_path.suffix))
            outputs.append(TaskOutput(
                type=f"lod_{result['label']}",
                format=fmt,
                key=dest_key,
                size_bytes=result["size_bytes"],
                metadata={
                    "triangle_count": result["triangle_count"],
                    "vertex_count":   result["vertex_count"],
                    "level":          result["level"],
                },
            ))
        return outputs
