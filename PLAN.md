# Plan: Microservicio de Procesamiento 3D — Vision S3D Converter

## Contexto

El software de visualización 3D (Laravel + Inertia + React + PostgreSQL) necesita procesar archivos masivos: nubes de puntos de hasta 50M vértices, GeoTIFF, mallas 3D, imágenes, videos y archivos comprimidos. Este procesamiento no puede hacerse en el mismo proceso de Laravel porque:
- Las tareas pueden durar 30-60 minutos
- Consumen CPU/RAM extrema (hasta 75% de un servidor de 128 cores)
- Necesitan scheduler inteligente para no saturar el servidor
- El usuario necesita ver progreso en tiempo real en la UI

La solución es un microservicio Python dedicado que recibe tareas vía RabbitMQ, las procesa con workers especializados, y notifica progreso vía Redis Pub/Sub → Laravel Reverb → React.

---

## Stack del Sistema Principal

- **Backend:** Laravel + Inertia + React + PostgreSQL
- **Storage:** Cloudflare R2 (compatible S3)
- **Servidor dedicado:** 32-128 cores, 64-256GB RAM, 1Gbps

---

## Decisiones Tecnológicas

| Componente | Tecnología | Razón |
|---|---|---|
| Cola de tareas | **RabbitMQ** | DLQ nativa, prioridades, routing flexible, UI de admin |
| Progreso en tiempo real | **Redis Pub/Sub** | Sub-ms de latencia, patrón pub/sub natural |
| Scheduler | **Python + psutil** | Control total del bin-packing adaptado al workload |
| Nubes de puntos 3D | **Open3D + PDAL** | Open3D para geometría/LOD, PDAL para pipeline |
| GeoTIFF | **GDAL + rasterio + rio-cogeo** | Estándar de industria, sin alternativa |
| Imágenes grandes | **pyvips** | 10x menos RAM que Pillow, streaming por tiles |
| Video | **python-ffmpeg** | FFmpeg es el estándar; ffmpeg-python está abandonado desde 2019 |
| Workers | **subprocess Python** | Aislamiento real de memoria, SIGTERM limpio, sin GIL |
| Storage | **boto3 + R2** | R2 es S3-compatible, boto3 es el SDK estándar |
| API de progreso | **FastAPI + uvicorn** | Async nativo, SSE built-in |

---

## Versiones del Stack

| Componente | Versión | Nota |
|---|---|---|
| **Python** | **3.12** | 3.13 no soportado por Open3D |
| FastAPI | 0.134.0 | |
| uvicorn | 0.41.0 | |
| pydantic | 2.12.5 | |
| pydantic-settings | 2.13.1 | |
| redis-py | 7.1.1 | |
| boto3 | 1.42.x | Se actualiza diariamente |
| psutil | 7.2.2 | |
| structlog | 25.5.0 | |
| numpy | 2.4.2 | Breaking changes vs 1.x, todos los paquetes ya migrados |
| httpx | 0.28.1 | |
| **RabbitMQ Docker** | **4.0.9** | `rabbitmq:4.0.9-management` |
| **Redis Docker** | **8.6.1** | JSON/Search/Bloom integrados nativamente |
| **GDAL Docker** | **3.12.2** | `ghcr.io/osgeo/gdal:ubuntu-full-latest` |
| Open3D | 0.19.0 | Solo hasta Python 3.12 |
| PDAL | 3.5.3 | Paquete: `pdal` (no python-pdal) |
| laspy | 2.7.0 | |
| py3dtiles | 12.0.0 | |
| pygltflib | 1.16.5 | |
| rasterio | 1.5.0 | Requiere Python >=3.12 y numpy >=2 |
| rio-cogeo | 7.0.1 | |
| Pillow | 12.1.1 | |
| pyvips | 3.1.1 | Requiere libvips en el sistema |
| **python-ffmpeg** | latest | Reemplaza ffmpeg-python (abandonado) |
| py7zr | 1.1.0 | |
| rarfile | 4.2 | Requiere unrar en el sistema |
| zstandard | 0.25.0 | |

> **Nota pika:** Se evaluará al iniciar Fase 1 si usar `pika 1.3.2` (síncrono, sin releases desde 2023) o migrar a `aio-pika` (async, activamente mantenido).

---

## Tipos de Tarea

```
point_cloud_lod          # LOD + tiles de nube de puntos (hasta 50M vértices)
point_cloud_convert      # Conversión de formato (LAS/LAZ/E57/XYZ/PTS/OBJ/GLB/PLY)
mesh_3d_lod              # LOD de malla 3D (OBJ/FBX → GLB con LOD)
mesh_3d_convert          # Conversión formato malla
geotiff_resize           # Redimensionado GeoTIFF masivo
geotiff_cog              # Cloud Optimized GeoTIFF + pirámides
image_convert            # Conversión JPG/PNG/TIFF/AVIF/WEBP
image_thumbnail          # Thumbnail de imagen
video_transcode          # Transcode + resize de video
archive_compress         # Comprimir archivos (ZIP/TAR/RAR/7ZIP)
archive_extract          # Descomprimir archivos
```

---

## Estructura de Directorios

```
vision-s3d-converter/
├── docker-compose.yml
├── .env.example
├── PLAN.md                          # Este archivo
│
├── orchestrator/
│   ├── Dockerfile
│   ├── main.py                      # Entry point
│   ├── config.py                    # pydantic-settings
│   ├── scheduler/
│   │   ├── resource_monitor.py      # psutil — CPU/RAM en tiempo real
│   │   ├── bin_packer.py            # Algoritmo de scheduling (CRÍTICO)
│   │   ├── task_weights.py          # Tabla de pesos por tipo de tarea
│   │   └── worker_pool.py           # Gestiona subprocesos activos
│   ├── queue/
│   │   ├── rabbitmq_client.py       # Conexión, declaración de colas, ACK/NACK
│   │   └── redis_client.py          # Redis Pub/Sub para progreso
│   ├── storage/
│   │   └── r2_client.py             # boto3 para Cloudflare R2
│   └── models/
│       └── task.py                  # Pydantic models de tareas
│
├── workers/
│   ├── base_worker.py               # Clase abstracta: checkpoint + SIGTERM + progreso
│   ├── point_cloud/
│   │   ├── worker.py
│   │   ├── lod_generator.py         # Open3D voxel downsampling
│   │   ├── format_converter.py      # LAS/LAZ/E57/XYZ/PTS/OBJ/GLB
│   │   └── tile_generator.py        # 3D Tiles (py3dtiles)
│   ├── geotiff/
│   │   ├── worker.py
│   │   ├── resizer.py               # rasterio para resize masivo
│   │   └── cog_generator.py         # Cloud Optimized GeoTIFF (rio-cogeo)
│   ├── image/
│   │   ├── worker.py
│   │   └── converter.py             # pyvips + Pillow: JPG/PNG/TIFF/AVIF/WEBP
│   ├── video/
│   │   ├── worker.py
│   │   └── converter.py             # ffmpeg-python
│   └── archive/
│       ├── worker.py
│       └── handler.py               # ZIP/TAR/RAR(rarfile)/7ZIP(py7zr)/ZST(zstandard)
│
├── progress/
│   ├── reporter.py                  # Redis Pub/Sub con rate-limiting 2s
│   └── sse_server.py                # FastAPI SSE (alternativa directa al frontend)
│
├── checkpoints/                     # Estado persistido para tareas largas
│
└── requirements/
    ├── base.txt                     # pika, redis, boto3, pydantic, psutil, fastapi
    ├── point_cloud.txt              # pdal, open3d, laspy, py3dtiles, pygltflib
    ├── geotiff.txt                  # gdal, rasterio, rio-cogeo
    ├── image.txt                    # Pillow, pyvips
    ├── video.txt                    # python-ffmpeg
    └── archive.txt                  # py7zr, rarfile, zstandard
```

---

## Esquemas JSON de Mensajes

### Tarea de entrada (Laravel → RabbitMQ → Microservicio)

```json
{
  "task_id": "uuid-v4",
  "tenant_id": "tenant-uuid",
  "file_id": "file-uuid-en-laravel",
  "task_type": "point_cloud_lod",
  "priority": 5,
  "submitted_at": "2026-02-28T10:00:00Z",
  "source": {
    "bucket": "vision-s3d-r2",
    "key": "tenants/abc123/files/scan.las",
    "size_bytes": 2147483648,
    "format": "las"
  },
  "destination": {
    "bucket": "vision-s3d-r2",
    "base_key": "tenants/abc123/processed/scan/"
  },
  "params": {
    "lod_levels": [
      { "level": 0, "max_points": 50000000, "label": "high" },
      { "level": 1, "max_points": 5000000, "label": "medium" },
      { "level": 2, "max_points": 500000, "label": "low" }
    ],
    "output_formats": ["laz", "glb"],
    "generate_thumbnails": true
  },
  "webhook": {
    "callback_queue": "vision-s3d.results",
    "progress_channel": "task:uuid-v4:progress"
  },
  "retry": { "attempt": 1, "max_attempts": 3 }
}
```

### Progreso (Worker → Redis Pub/Sub, canal `task:{task_id}:progress`)

```json
{
  "task_id": "uuid-v4",
  "tenant_id": "tenant-uuid",
  "file_id": "file-uuid-en-laravel",
  "status": "processing",
  "progress": {
    "percent": 42,
    "stage": "lod_generation",
    "stage_label": "Generando LOD nivel 2 de 3",
    "items_done": 21000000,
    "items_total": 50000000
  },
  "eta_seconds": 620
}
```

### Resultado final (Worker → RabbitMQ cola `vision-s3d.results`)

```json
{
  "task_id": "uuid-v4",
  "tenant_id": "tenant-uuid",
  "file_id": "file-uuid-en-laravel",
  "status": "completed",
  "completed_at": "2026-02-28T10:35:12Z",
  "duration_seconds": 2112,
  "outputs": [
    { "type": "lod_high", "format": "laz", "key": "tenants/.../lod_high.laz", "size_bytes": 892334080 },
    { "type": "lod_medium", "format": "laz", "key": "tenants/.../lod_medium.laz", "size_bytes": 89233408 },
    { "type": "thumbnail", "format": "webp", "key": "tenants/.../thumbnail.webp", "size_bytes": 45000 }
  ]
}
```

### Error con salidas parciales

```json
{
  "task_id": "uuid-v4",
  "status": "failed",
  "error": {
    "code": "CONVERSION_FAILED",
    "message": "PDAL pipeline failed: unsupported LAS version 1.5",
    "stage": "format_detection",
    "recoverable": false
  },
  "partial_outputs": [...],
  "retry": { "attempt": 2, "will_retry": true }
}
```

---

## Scheduler: Algoritmo Bin-Packing

**Target de utilización: 85% CPU y 85% RAM**

### Tabla de pesos (`orchestrator/scheduler/task_weights.py`)

| task_type | CPU% | RAM GB | Concurrencia máx |
|---|---|---|---|
| point_cloud_lod | 75 | 32 | 1 |
| point_cloud_convert | 40 | 16 | 2 |
| mesh_3d_lod | 60 | 24 | 2 |
| mesh_3d_convert | 30 | 8 | 4 |
| geotiff_resize | 35 | 12 | 3 |
| geotiff_cog | 45 | 16 | 2 |
| image_convert | 5 | 1 | 20 |
| image_thumbnail | 3 | 0.5 | 30 |
| video_transcode | 50 | 4 | 3 |
| archive_compress | 20 | 2 | 5 |
| archive_extract | 15 | 2 | 5 |

### Lógica del ciclo (cada 2 segundos en `bin_packer.py`)

1. Leer snapshot real de CPU/RAM con psutil
2. Calcular `disponible = 85% - uso_actual`
3. Ordenar cola: `priority DESC, submitted_at ASC` (FIFO por prioridad)
4. Primera pasada: tareas pesadas (CPU ≥ 30%) que quepan → First Fit Decreasing
5. Segunda pasada: rellenar espacio restante con tareas ligeras
6. Respetar `concurrency_limit` por tipo de tarea
7. Lanzar cada tarea como subproceso Python independiente

---

## Propagación de Progreso en Tiempo Real

```
Worker Python
    → Redis Pub/Sub (canal task:{task_id}:progress)
    → Laravel Artisan Command (processing:listen-progress)
    → broadcast(FileProcessingProgress) via Laravel Reverb
    → React hook useFileProcessingProgress(fileId)
    → <ProcessingProgressBar percent={42} />
```

- Rate limit: máximo 1 mensaje de progreso cada 2 segundos por tarea
- Estado persistido en Redis con TTL 1 hora (`task:{id}:last_progress`) para reconexiones
- Canal privado Reverb: `tenant.{tenantId}.files`

---

## Resiliencia

### Estructura de colas RabbitMQ

```
Exchange: vision.s3d.tasks (topic)
  ├── tasks.high_priority  (point_cloud_lod, mesh_3d_lod)
  ├── tasks.normal         (geotiff, video, conversiones 3D)
  └── tasks.light          (image, archive)
        Todas con DLX → tasks.dlq después de 3 intentos

Queue: tasks.dlq (Dead Letter Queue, TTL 7 días)
  → Laravel consumer marca file como error permanente y notifica al usuario

Exchange: vision.s3d.results (direct)
  └── results → Laravel consumer actualiza modelo File
```

### Reintentos con backoff exponencial

| Intento | Delay antes de reintentar |
|---|---|
| 1 | Inmediato |
| 2 | +30 segundos |
| 3 | +60 segundos |
| 4+ | DLQ → error permanente |

### Checkpointing para tareas largas

- `base_worker.py` guarda JSON en `/app/checkpoints/{task_id}.json` después de cada etapa
- Handler SIGTERM guarda checkpoint antes de morir
- Al recibir tarea con retry > 1, el worker carga el checkpoint y retoma desde la última etapa

---

## Integración con Laravel

Componentes que Laravel necesita agregar:

1. **Job** para publicar tareas en RabbitMQ al completar upload de un archivo
2. **Artisan Command** `processing:listen-progress` — subscriber Redis → Reverb broadcast
3. **Artisan Command** `processing:listen-results` — consumer de `vision-s3d.results` → actualiza modelo `File`
4. **Evento** `FileProcessingProgress` (ShouldBroadcast) en canal privado de tenant
5. **Evento** `FileProcessingCompleted` para notificación final al usuario

---

## Docker Compose

Servicios:
- `rabbitmq` — imagen `rabbitmq:4.0.9-management`
- `redis` — imagen `redis:8.6.1` (JSON/Search/Bloom integrados, sin Redis Stack separado)
- `orchestrator` — imagen base `ghcr.io/osgeo/gdal:ubuntu-full-latest` (GDAL 3.12.2 precompilado)
- `progress-sse` — FastAPI SSE server en puerto 8080

Volúmenes: `rabbitmq_data`, `redis_data`, `checkpoints`, `/tmp/vs3d_processing`

> **Nota GDAL:** La imagen oficial `ghcr.io/osgeo/gdal:ubuntu-full-latest` evita semanas de problemas de compilación. Instalar rasterio con `--no-binary rasterio` para evitar conflictos con el GDAL del sistema.
> **Nota disco:** Los archivos temporales (un LAS descomprimido puede ser 20GB) deben montarse en un volumen del host con suficiente espacio, no en el filesystem del contenedor.

---

## Plan de Implementación por Fases

### ✅ Fase 0: Planeación (Completada)

### Fase 1: Pipeline base funcional (Semana 1-2)

- [ ] Docker Compose: RabbitMQ + Redis levantados y configurados
- [ ] `orchestrator/config.py` con pydantic-settings y `.env.example`
- [ ] `orchestrator/queue/rabbitmq_client.py` — conectar, declarar colas con DLX
- [ ] `orchestrator/storage/r2_client.py` — upload/download desde R2
- [ ] `workers/base_worker.py` — clase abstracta con checkpoint y SIGTERM handler
- [ ] `workers/image/` — conversión de imagen (tarea más simple, sin dependencias pesadas)
- [ ] `progress/reporter.py` — publicar progreso en Redis Pub/Sub
- [ ] **Hito:** Laravel publica tarea de imagen → microservicio procesa → sube a R2 → resultado en cola

### Fase 2: Scheduler inteligente (Semana 2-3)

- [ ] `scheduler/resource_monitor.py` con psutil
- [ ] `scheduler/task_weights.py` con tabla de pesos
- [ ] `scheduler/bin_packer.py` con algoritmo First Fit Decreasing
- [ ] `scheduler/worker_pool.py` — gestión de subprocesos
- [ ] **Hito:** 50 tareas de imagen + 1 tarea pesada simulada → scheduler no satura el servidor

### Fase 3: Progreso en tiempo real (Semana 3-4)

- [ ] `progress/sse_server.py` (FastAPI)
- [ ] Laravel: `FileProcessingProgress` event + `listen-progress` command
- [ ] Configurar Laravel Reverb
- [ ] React: hook `useFileProcessingProgress` + componente `ProcessingProgressBar`
- [ ] **Hito:** Usuario ve barra de progreso en vivo mientras se procesa un archivo

### Fase 4: Nubes de puntos y mallas 3D (Semana 4-6)

- [ ] Imagen Docker con PDAL + Open3D compilados
- [ ] `workers/point_cloud/format_converter.py` — LAS/LAZ/E57/XYZ/PTS/OBJ/GLB
- [ ] `workers/point_cloud/lod_generator.py` — voxel downsampling con Open3D
- [ ] `workers/point_cloud/tile_generator.py` — 3D Tiles con py3dtiles
- [ ] Checkpointing por nivel de LOD
- [ ] `workers/mesh_3d/` — OBJ/FBX → GLB con simplificación
- [ ] **Hito:** Archivo LAS de 50M puntos → 3 niveles de LOD + GLB para visor web

### Fase 5: GeoTIFF y Video (Semana 6-7)

- [ ] `workers/geotiff/resizer.py` con rasterio
- [ ] `workers/geotiff/cog_generator.py` con rio-cogeo
- [ ] `workers/video/converter.py` con python-ffmpeg
- [ ] `workers/archive/handler.py` — ZIP/TAR/RAR/7ZIP
- [ ] **Hito:** Todos los tipos de archivo soportados

### Fase 6: Resiliencia y producción (Semana 7-8)

- [ ] DLQ completamente funcional con consumer Laravel
- [ ] Plugin `rabbitmq-delayed-message-exchange` para backoff exponencial
- [ ] Logging estructurado JSON con structlog
- [ ] Endpoint `/health` para monitoreo externo
- [ ] Load testing: 100 tareas concurrentes de tipos mixtos
- [ ] **Hito:** Sistema estable en producción

---

## Verificación End-to-End

1. `docker-compose up` → todos los servicios levantados y healthy
2. Publicar manualmente un mensaje JSON en RabbitMQ Management UI (`:15672`)
3. Ver en logs del orchestrator que la tarea es tomada y asignada al worker correcto
4. Ver en Redis `SUBSCRIBE task:*:progress` que los mensajes de progreso llegan
5. Verificar que el archivo resultado aparece en R2 en la ruta destino
6. Ver en la cola `vision-s3d.results` el mensaje de resultado final
