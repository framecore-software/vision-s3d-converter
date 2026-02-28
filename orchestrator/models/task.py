from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from pydantic import BaseModel, Field
import uuid


class TaskType(StrEnum):
    POINT_CLOUD_LOD = "point_cloud_lod"
    POINT_CLOUD_CONVERT = "point_cloud_convert"
    MESH_3D_LOD = "mesh_3d_lod"
    MESH_3D_CONVERT = "mesh_3d_convert"
    GEOTIFF_RESIZE = "geotiff_resize"
    GEOTIFF_COG = "geotiff_cog"
    IMAGE_CONVERT = "image_convert"
    IMAGE_THUMBNAIL = "image_thumbnail"
    VIDEO_TRANSCODE = "video_transcode"
    ARCHIVE_COMPRESS = "archive_compress"
    ARCHIVE_EXTRACT = "archive_extract"


class TaskStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskSource(BaseModel):
    bucket: str
    key: str
    size_bytes: int = 0
    format: str = ""


class TaskDestination(BaseModel):
    bucket: str
    base_key: str


class TaskWebhook(BaseModel):
    callback_queue: str = "vision-s3d.results"
    progress_channel: str = ""

    def model_post_init(self, __context: Any) -> None:
        # El canal de progreso se auto-genera si no se especifica
        if not self.progress_channel and hasattr(self, "_task_id"):
            self.progress_channel = f"task:{self._task_id}:progress"


class TaskRetry(BaseModel):
    attempt: int = 1
    max_attempts: int = 3
    previous_error: str | None = None


class TaskOutput(BaseModel):
    type: str
    format: str
    key: str
    size_bytes: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class Task(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    file_id: str
    task_type: TaskType
    priority: int = Field(default=5, ge=1, le=10)
    submitted_at: datetime = Field(default_factory=datetime.utcnow)
    source: TaskSource
    destination: TaskDestination
    params: dict[str, Any] = Field(default_factory=dict)
    webhook: TaskWebhook = Field(default_factory=TaskWebhook)
    retry: TaskRetry = Field(default_factory=TaskRetry)

    def model_post_init(self, __context: Any) -> None:
        # Asegurar que el canal de progreso tenga el task_id
        if not self.webhook.progress_channel:
            self.webhook.progress_channel = f"task:{self.task_id}:progress"


class TaskResult(BaseModel):
    task_id: str
    tenant_id: str
    file_id: str
    status: TaskStatus
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    duration_seconds: float = 0.0
    outputs: list[TaskOutput] = Field(default_factory=list)
    total_output_bytes: int = 0
    worker_info: dict[str, Any] = Field(default_factory=dict)


class TaskError(BaseModel):
    task_id: str
    tenant_id: str
    file_id: str
    status: TaskStatus = TaskStatus.FAILED
    failed_at: datetime = Field(default_factory=datetime.utcnow)
    error: dict[str, Any]
    partial_outputs: list[TaskOutput] = Field(default_factory=list)
    retry: TaskRetry


class ProgressUpdate(BaseModel):
    task_id: str
    tenant_id: str
    file_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: TaskStatus = TaskStatus.PROCESSING
    percent: int = Field(ge=0, le=100)
    stage: str
    stage_label: str
    items_done: int = 0
    items_total: int = 0
    eta_seconds: int | None = None
