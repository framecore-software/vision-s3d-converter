# TODO — Vision S3D Converter

Pendientes de implementación, mejoras identificadas y trabajo de integración.
Los ítems completados se eliminan de esta lista; ver el historial de commits para el detalle.

---

## Sprint 3 — Tests unitarios del bin-packer (siguiente prioridad)

El bin-packer es el componente más crítico del sistema y no tiene ningún test.
Un bug aquí puede saturar el servidor en producción.

Crear `tests/scheduler/test_bin_packer.py` con pytest:

- [ ] **Sin recursos disponibles** → `pack()` retorna `[]`
- [ ] **`point_cloud_lod` (parallelizable=False)** bloquea el lanzamiento de una segunda
      tarea no-paralelizable, pero permite tareas ligeras simultáneas
- [ ] **`concurrency_max` se respeta** — si ya hay 2 `mesh_3d_lod` en ejecución, no se lanza un tercero
- [ ] **`MAX_CONCURRENT_WORKERS = 50`** — con 50 workers activos, `pack()` retorna `[]`
      independientemente de los recursos disponibles
- [ ] **Prioridad DESC + FIFO** — con dos tareas del mismo tipo, la de mayor prioridad
      se programa primero; con igual prioridad, la más antigua
- [ ] **Pasada pesada + ligera** — verificar que las tareas ligeras rellenan el espacio
      restante tras colocar las tareas pesadas

Crear también:
- [ ] `tests/scheduler/test_resource_monitor.py` — verificar que el snapshot devuelve
      valores dentro de rango razonable (0-100% CPU, > 0 GB RAM)
- [ ] `tests/workers/test_archive_handler.py` — path traversal attempt en ZIP y RAR
      debe lanzar `ValueError`; ZIP-bomb debe lanzar `ValueError` antes de agotar el disco

---

## Sprint 5 — Integración con Laravel (repositorio separado)

Estos componentes viven en el proyecto Laravel y son necesarios para el flujo completo.
Orden recomendado porque cada paso es verificable de forma independiente:

- [ ] **Job `DispatchProcessingTask`** — publica la tarea en RabbitMQ al completar un upload.
      Verificar con Management UI (`:15672`).
- [ ] **Artisan Command `processing:listen-results`** — consumer de `vision-s3d.results`;
      actualiza el modelo `File` con `status`, `outputs` y `processed_at`.
- [ ] **Artisan Command `processing:listen-progress`** — subscriber Redis;
      retransmite mensajes de progreso vía Reverb.
- [ ] **Evento `FileProcessingProgress`** (ShouldBroadcast) en canal privado `tenant.{id}.files`
- [ ] **Evento `FileProcessingCompleted`** — notificación final al usuario
- [ ] **Consumer de DLQ** (`tasks.dlq`) — detecta fallos permanentes y marca el `File`
      como `processing_failed`
- [ ] **React hook `useFileProcessingProgress(fileId)`** — consume SSE o Reverb
- [ ] **Componente `<ProcessingProgressBar />`** — barra de progreso con estado, etapa y ETA

---

## Sprint 6 — Observabilidad básica

Solo lo esencial para producción; tracing y logging centralizado se dejan para después.

- [ ] **Exportador Prometheus** — el endpoint `/metrics` devuelve JSON, pero no en formato
      `text/plain` de Prometheus. Instalar `prometheus-client` en `base.txt` y agregar
      un endpoint `GET /metrics/prometheus` con counters de tareas completadas/fallidas
      por tipo e histograma de duración.
- [ ] **Dockerfile del orchestrator** — la imagen base usa `ghcr.io/osgeo/gdal:ubuntu-full-latest`
      (tag flotante). Pinear a una versión específica para builds reproducibles
      (e.g. `ubuntu-full-3.12.2`). Verificar que `unrar-free` está instalado.
- [ ] **Dashboard Grafana** — crear `grafana/dashboards/vs3d.json` commiteado en el repo
      con: workers activos, throughput por tipo de tarea, CPU/RAM del servidor.
- [ ] **Alertas** — definir en Grafana/Alertmanager: DLQ con mensajes pendientes,
      CPU > 95% por más de 5 minutos, Redis o RabbitMQ no responden.

Backlog (sin fecha urgente):
- [ ] **Tracing distribuido** — `opentelemetry-python` para trazar el flujo
      tarea → worker → resultado
- [ ] **Logging centralizado** — driver Docker para enviar los logs JSON estructurados a Loki

---

## Sprint 7 — Load test (en staging)

Ejecutar solo después de que el stack completo funcione (Laravel + microservicio).

- [ ] Script Python que publique 100 tareas mixtas en RabbitMQ simultáneamente
- [ ] Verificar que el scheduler respeta el 85% de CPU/RAM con Grafana
- [ ] Verificar que `MAX_CONCURRENT_WORKERS = 50` se respeta (nunca más de 50 procesos)
- [ ] Medir latencia p50/p95 desde que se publica la tarea hasta que llega el resultado
- [ ] Verificar que un rolling restart del orchestrator durante el test no pierde tareas
      (gracias al graceful shutdown del Sprint 4)

---

## Sprint 8 — Mejoras opcionales (backlog sin fecha)

Bajo impacto inmediato; se hacen cuando haya tiempo.

### Workers

- [ ] **`PointCloudWorker`**: `py3dtiles_convert` usa `jobs=4` fijo. Considerar leerlo
      de la configuración o derivarlo del número de CPUs disponibles.
- [ ] **`GeoTiffWorker`**: agregar reproyección CRS a EPSG:4326 o EPSG:3857 antes
      de generar el COG (`rasterio.warp.reproject`), ya que los visores web
      esperan coordenadas geográficas.
- [ ] **`Mesh3DWorker`**: agregar validación post-decimación QEM — verificar que el mesh
      resultante no tiene triángulos degenerados ni vértices aislados.
- [ ] **`VideoWorker`**: investigar si FFmpeg soporta reanudar una transcodificación
      desde un archivo parcial para mejorar el checkpointing.
- [ ] **`ArchiveWorker`**: agregar soporte para compresión `.tar.bz2` y `.tar.xz`
      en `_compress_tar()`.

### Infraestructura

- [ ] **Multi-stage Docker build** — separar la etapa de compilación de la imagen final
      para reducir el tamaño del contenedor del orchestrator.
- [ ] **Separar imágenes por tipo de worker** — una imagen con solo las dependencias
      de imagen/archivo, otra con GDAL, otra con Open3D, etc. Reduce el tamaño
      y mejora los tiempos de build y pull.
- [ ] **Kubernetes readiness probe** — el `/health` ya sirve como liveness probe;
      agregar un readiness probe separado que verifique si el orchestrator está
      listo para aceptar tareas (conexión RabbitMQ establecida).

### Código

- [ ] **`rabbitmq_client.py`**: verificar que la lógica de routing por prioridad es correcta
      cuando hay tareas en múltiples colas simultáneamente (routing key `tasks.high.*`
      vs `tasks.normal.*` vs `tasks.light.*`).
- [ ] **`progress/reporter.py`**: con 50 workers simultáneos puede haber hasta 50
      publicaciones Redis en el mismo segundo. Evaluar si Redis lo tolera sin problemas
      o si hace falta un rate-limit global adicional.
