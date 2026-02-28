# ─────────────────────────────────────────────
# Imagen base: GDAL 3.12 precompilado (Ubuntu)
# Incluye GDAL, PROJ, GEOS y todas sus extensiones C++
# Evita horas de compilación manual
# ─────────────────────────────────────────────
FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Python 3.12 (requerido por Open3D — no soporta 3.13)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    python3-pip \
    # libvips para pyvips
    libvips-dev \
    # unrar para rarfile
    unrar \
    # ffmpeg para python-ffmpeg
    ffmpeg \
    # curl para healthcheck
    curl \
    # Limpieza
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Usar python3.12 como python por defecto
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# Actualizar pip
RUN python -m pip install --upgrade pip

WORKDIR /app

# Instalar dependencias base primero (mejor uso de caché Docker)
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/base.txt

# Instalar dependencias de imagen
COPY requirements/image.txt requirements/image.txt
RUN pip install --no-cache-dir -r requirements/image.txt

# Instalar rasterio sin binaries bundled para usar el GDAL del sistema
COPY requirements/geotiff.txt requirements/geotiff.txt
RUN pip install --no-cache-dir --no-binary rasterio -r requirements/geotiff.txt

# Instalar video y archive
COPY requirements/video.txt requirements/video.txt
COPY requirements/archive.txt requirements/archive.txt
RUN pip install --no-cache-dir -r requirements/video.txt \
    && pip install --no-cache-dir -r requirements/archive.txt

# Copiar código fuente
COPY orchestrator/ orchestrator/
COPY workers/ workers/
COPY progress/ progress/

# Directorios de datos
RUN mkdir -p /app/checkpoints /tmp/processing

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

CMD ["python", "-m", "orchestrator.main"]
