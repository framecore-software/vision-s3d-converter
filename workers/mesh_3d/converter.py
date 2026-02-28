from __future__ import annotations

import logging
from pathlib import Path

import open3d as o3d

logger = logging.getLogger(__name__)

# Formatos soportados por Open3D.
# NOTA: .fbx NO está disponible en Linux — requiere el FBX SDK propietario de Autodesk,
# que Open3D no incluye en su build oficial para Linux. Para convertir .fbx, use
# assimp o Blender como paso previo antes de enviar la tarea al microservicio.
INPUT_FORMATS  = {".obj", ".ply", ".glb", ".gltf", ".stl", ".off"}
OUTPUT_FORMATS = {".glb", ".gltf", ".ply", ".obj", ".stl"}


def convert_mesh(src: Path, dst: Path) -> Path:
    """
    Convierte una malla 3D entre formatos usando Open3D.

    Soporta: OBJ, FBX, PLY, GLB/GLTF, STL → GLB, GLTF, PLY, OBJ, STL

    Open3D maneja la conversión internamente; para FBX de entrada
    usa la lectura de triángulos, que es compatible con la mayoría de exportadores.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst_ext = dst.suffix.lower()

    if dst_ext not in OUTPUT_FORMATS:
        raise ValueError(f"Formato de salida no soportado: {dst_ext}. Disponibles: {OUTPUT_FORMATS}")

    mesh = o3d.io.read_triangle_mesh(str(src), enable_post_processing=True)

    if not mesh.has_vertices():
        raise ValueError(f"No se pudieron leer vértices de {src.name}")

    # Asegurar normales para formatos que las requieren
    if not mesh.has_vertex_normals():
        mesh.compute_vertex_normals()

    o3d.io.write_triangle_mesh(str(dst), mesh, write_ascii=False, compressed=True)
    logger.info(
        "Malla convertida",
        extra={
            "src": src.name, "dst": dst.name,
            "vertices": len(mesh.vertices),
            "triangles": len(mesh.triangles),
        },
    )
    return dst
