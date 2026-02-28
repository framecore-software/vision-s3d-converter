from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika
import aio_pika.abc

from orchestrator.config import settings
from orchestrator.models.task import Task, TaskType

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Nombres de exchanges y colas
# ─────────────────────────────────────────────
EXCHANGE_TASKS = "vision.s3d.tasks"
EXCHANGE_TASKS_DELAYED = "vision.s3d.tasks.delayed"
EXCHANGE_RESULTS = "vision.s3d.results"
EXCHANGE_DLX = "vision.s3d.dlx"

# Delays de backoff exponencial por intento (en ms)
_RETRY_DELAYS_MS = {
    2: 30_000,    # intento 2: +30s
    3: 60_000,    # intento 3: +60s
}

QUEUE_HIGH = "tasks.high_priority"
QUEUE_NORMAL = "tasks.normal"
QUEUE_LIGHT = "tasks.light"
QUEUE_RESULTS = "vision-s3d.results"
QUEUE_DLQ = "tasks.dlq"

# Tipos de tarea por cola
_HIGH_PRIORITY_TYPES = {TaskType.POINT_CLOUD_LOD, TaskType.MESH_3D_LOD}
_LIGHT_TYPES = {TaskType.IMAGE_CONVERT, TaskType.IMAGE_THUMBNAIL, TaskType.ARCHIVE_COMPRESS, TaskType.ARCHIVE_EXTRACT}


def queue_for_task(task_type: TaskType) -> str:
    if task_type in _HIGH_PRIORITY_TYPES:
        return QUEUE_HIGH
    if task_type in _LIGHT_TYPES:
        return QUEUE_LIGHT
    return QUEUE_NORMAL


# ─────────────────────────────────────────────
# Setup de infraestructura (colas + exchanges)
# ─────────────────────────────────────────────

async def setup_queues(channel: aio_pika.abc.AbstractChannel) -> None:
    """Declara todos los exchanges y colas con Dead Letter Exchange configurado."""

    # Dead Letter Exchange (recibe mensajes rechazados definitivamente)
    dlx = await channel.declare_exchange(EXCHANGE_DLX, aio_pika.ExchangeType.DIRECT, durable=True)

    # Dead Letter Queue (TTL 7 días — Laravel consumer la supervisa)
    dlq = await channel.declare_queue(
        QUEUE_DLQ,
        durable=True,
        arguments={"x-message-ttl": 604_800_000},
    )
    await dlq.bind(dlx, routing_key="dead")

    # Exchange principal de tareas (topic — permite routing por task_type)
    tasks_exchange = await channel.declare_exchange(EXCHANGE_TASKS, aio_pika.ExchangeType.TOPIC, durable=True)

    # Exchange delayed para reintentos con backoff exponencial
    # Requiere el plugin rabbitmq_delayed_message_exchange
    delayed_exchange = await channel.declare_exchange(
        EXCHANGE_TASKS_DELAYED,
        aio_pika.ExchangeType.X_DELAYED_MESSAGE,
        durable=True,
        arguments={"x-delayed-type": "topic"},
    )

    # Exchange de resultados (direct)
    await channel.declare_exchange(EXCHANGE_RESULTS, aio_pika.ExchangeType.DIRECT, durable=True)

    # Cola de resultados (consumida por Laravel)
    results_queue = await channel.declare_queue(QUEUE_RESULTS, durable=True)
    results_exchange = await channel.get_exchange(EXCHANGE_RESULTS)
    await results_queue.bind(results_exchange, routing_key=QUEUE_RESULTS)

    # Colas de tareas con DLX configurado
    queue_configs = [
        (QUEUE_HIGH,   "tasks.high.*",   86_400_000),    # 24h TTL
        (QUEUE_NORMAL, "tasks.normal.*", 43_200_000),    # 12h TTL
        (QUEUE_LIGHT,  "tasks.light.*",  21_600_000),    # 6h TTL
    ]

    for queue_name, routing_key, ttl_ms in queue_configs:
        q = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": EXCHANGE_DLX,
                "x-dead-letter-routing-key": "dead",
                "x-message-ttl": ttl_ms,
            },
        )
        await q.bind(tasks_exchange, routing_key=routing_key)
        # El delayed exchange enruta a las mismas colas tras el delay
        await q.bind(delayed_exchange, routing_key=routing_key)

    logger.info("RabbitMQ: exchanges y colas declarados correctamente")


# ─────────────────────────────────────────────
# Publicar una tarea (usado por Laravel o tests)
# ─────────────────────────────────────────────

async def publish_task(connection: aio_pika.abc.AbstractConnection, task: Task) -> None:
    async with connection.channel() as channel:
        exchange = await channel.get_exchange(EXCHANGE_TASKS)
        queue_name = queue_for_task(task.task_type)
        routing_key = queue_name.replace("tasks.", "tasks.") + f".{task.task_type}"

        body = task.model_dump_json().encode()
        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await exchange.publish(message, routing_key=routing_key)
        logger.info("Tarea publicada", extra={"task_id": task.task_id, "task_type": task.task_type})


# ─────────────────────────────────────────────
# Publicar reintento con backoff exponencial
# ─────────────────────────────────────────────

async def publish_retry(
    connection: aio_pika.abc.AbstractConnection,
    task: Task,
    error: str,
) -> bool:
    """
    Republicar una tarea fallida con backoff exponencial usando el delayed exchange.

    Returns:
        True si se programó el reintento, False si se agotaron los intentos (→ DLQ).
    """
    next_attempt = task.retry.attempt + 1

    if next_attempt > task.retry.max_attempts:
        logger.warning(
            "Tarea agotó reintentos, enviando a DLQ",
            extra={"task_id": task.task_id, "attempts": task.retry.attempt},
        )
        async with connection.channel() as channel:
            dlx = await channel.get_exchange(EXCHANGE_DLX)
            body = task.model_dump_json().encode()
            msg = aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                headers={"x-death-reason": error, "x-final-attempt": task.retry.attempt},
            )
            await dlx.publish(msg, routing_key="dead")
        return False

    delay_ms = _RETRY_DELAYS_MS.get(next_attempt, 120_000)
    task.retry.attempt = next_attempt
    task.retry.previous_error = error

    async with connection.channel() as channel:
        delayed_exchange = await channel.get_exchange(EXCHANGE_TASKS_DELAYED)
        queue_name = queue_for_task(task.task_type)
        routing_key = queue_name.replace("tasks.", "tasks.") + f".{task.task_type}"

        body = task.model_dump_json().encode()
        msg = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            headers={"x-delay": delay_ms},
        )
        await delayed_exchange.publish(msg, routing_key=routing_key)

    logger.info(
        "Reintento programado",
        extra={
            "task_id":    task.task_id,
            "attempt":    next_attempt,
            "delay_ms":   delay_ms,
            "task_type":  task.task_type,
        },
    )
    return True


# ─────────────────────────────────────────────
# Publicar resultado (usado por el orchestrator al finalizar)
# ─────────────────────────────────────────────

async def publish_result(channel: aio_pika.abc.AbstractChannel, result: dict[str, Any]) -> None:
    exchange = await channel.get_exchange(EXCHANGE_RESULTS)
    body = json.dumps(result, default=str).encode()
    message = aio_pika.Message(
        body=body,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
    )
    await exchange.publish(message, routing_key=QUEUE_RESULTS)


# ─────────────────────────────────────────────
# Consumir tareas
# ─────────────────────────────────────────────

MessageHandler = Callable[[Task, aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]


async def consume_tasks(
    connection: aio_pika.abc.AbstractConnection,
    handler: MessageHandler,
    queues: list[str] | None = None,
) -> None:
    """
    Inicia el consumo de tareas desde las colas especificadas.
    El handler recibe la Task ya deserializada y el mensaje original
    para hacer ACK/NACK manualmente.
    """
    queues = queues or [QUEUE_HIGH, QUEUE_NORMAL, QUEUE_LIGHT]
    channel = await connection.channel()

    # prefetch=1 para que el scheduler controle cuántas tareas toma
    await channel.set_qos(prefetch_count=1)

    async def _on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            task = Task.model_validate_json(message.body)
        except Exception as exc:
            logger.error("Mensaje inválido, rechazando sin requeue", exc_info=exc)
            await message.reject(requeue=False)
            return

        try:
            await handler(task, message)
        except Exception as exc:
            logger.error(
                "Error procesando tarea",
                extra={"task_id": task.task_id},
                exc_info=exc,
            )
            # NACK sin requeue — el orchestrator decide si republicar con retry
            await message.nack(requeue=False)

    for queue_name in queues:
        queue = await channel.get_queue(queue_name)
        await queue.consume(_on_message)
        logger.info(f"Consumiendo cola: {queue_name}")


# ─────────────────────────────────────────────
# Conexión
# ─────────────────────────────────────────────

async def connect() -> aio_pika.abc.AbstractRobustConnection:
    """Crea una conexión robusta (reconexión automática) a RabbitMQ."""
    connection = await aio_pika.connect_robust(
        settings.rabbitmq_url,
        reconnect_interval=5,
    )
    async with connection.channel() as ch:
        await setup_queues(ch)
    logger.info("Conectado a RabbitMQ")
    return connection
