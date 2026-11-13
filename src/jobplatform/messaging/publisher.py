"""Confirmed publishing to RabbitMQ."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from jobplatform.config import Settings
from jobplatform.messaging.topology import (
    DEAD_ROUTING_KEY,
    WORK_ROUTING_KEY,
    Topology,
    declare_topology,
)


class ConfirmedPublisher:
    """Publishes persistent messages and waits for the broker's publisher confirm.

    mandatory=True makes an unroutable message fail loudly (basic.return) instead of being
    silently dropped, so a missing binding can never turn into data loss.
    """

    def __init__(self, settings: Settings, confirm_timeout: float = 10.0) -> None:
        self._settings = settings
        self._timeout = confirm_timeout
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._topology: Topology | None = None

    async def start(self) -> None:
        connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        try:
            channel = await connection.channel(publisher_confirms=True)
            self._topology = await declare_topology(channel, self._settings)
        except BaseException:
            await connection.close()
            raise
        self._connection = connection
        self._channel = channel

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def publish(
        self, kind: str, body: dict[str, Any], priority: int, message_id: str
    ) -> None:
        if self._topology is None:
            raise RuntimeError("publisher not started")
        message = aio_pika.Message(
            body=json.dumps(body).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=priority,
            message_id=message_id,
        )
        if kind == "dlq":
            exchange, routing_key = self._topology.dlx, DEAD_ROUTING_KEY
        else:
            exchange, routing_key = self._topology.exchange, WORK_ROUTING_KEY
        await asyncio.wait_for(
            exchange.publish(message, routing_key=routing_key, mandatory=True),
            timeout=self._timeout,
        )
