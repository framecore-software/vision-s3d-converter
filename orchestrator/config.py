from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # RabbitMQ
    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        description="URL de conexión AMQP a RabbitMQ",
    )

    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="URL de conexión a Redis",
    )

    # Cloudflare R2 (opcionales — el servidor SSE no los necesita)
    r2_endpoint: str = Field(default="", description="Endpoint HTTPS de Cloudflare R2")
    r2_access_key: str = Field(default="", description="Access Key ID de R2")
    r2_secret_key: str = Field(default="", description="Secret Access Key de R2")
    r2_bucket: str = Field(default="", description="Nombre del bucket en R2")

    # Scheduler
    max_cpu_percent: float = Field(
        default=85.0,
        ge=10.0,
        le=99.0,
        description="Porcentaje máximo de CPU antes de dejar de asignar tareas",
    )
    max_ram_percent: float = Field(
        default=85.0,
        ge=10.0,
        le=99.0,
        description="Porcentaje máximo de RAM antes de dejar de asignar tareas",
    )
    scheduler_cycle_seconds: float = Field(
        default=2.0,
        ge=0.5,
        description="Intervalo en segundos del ciclo del scheduler",
    )

    # Almacenamiento temporal
    tmp_processing_dir: str = Field(
        default="/tmp/processing",
        description="Directorio para archivos temporales durante el procesamiento",
    )
    checkpoint_dir: str = Field(
        default="/app/checkpoints",
        description="Directorio para checkpoints de tareas largas",
    )

    # Servidor de salud
    health_port: int = Field(
        default=9090,
        description="Puerto del endpoint /health del orchestrator",
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Nivel de logging",
    )


settings = Settings()
