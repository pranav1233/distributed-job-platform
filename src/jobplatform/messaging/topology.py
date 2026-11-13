"""RabbitMQ topology: durable direct exchange, priority work queue, dead-letter exchange.

    jobs (direct) --work--> jobs.work (priority, DLX = jobs.dlx)
    jobs.dlx (direct) --dead--> jobs.dlq (TTL-bounded inspection copy)

The work queue dead-letters rejected (poison) messages to the DLX. Exhausted jobs are
published to the DLX by the relay through the outbox, so the DLQ copy is as durable as
the dispatch path.
"""

from __future__ import annotations

from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

from jobplatform.config import Settings

WORK_ROUTING_KEY = "work"
DEAD_ROUTING_KEY = "dead"


@dataclass(frozen=True, slots=True)
class Topology:
    exchange: AbstractExchange
    dlx: AbstractExchange
    work_queue: AbstractQueue
    dlq: AbstractQueue


async def declare_topology(channel: AbstractChannel, settings: Settings) -> Topology:
    exchange = await channel.declare_exchange(
        settings.exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlx = await channel.declare_exchange(
        settings.dlx_name, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlq = await channel.declare_queue(
        settings.dlq_name,
        durable=True,
        arguments={"x-message-ttl": settings.dlq_message_ttl_ms},
    )
    await dlq.bind(dlx, routing_key=DEAD_ROUTING_KEY)
    work_queue = await channel.declare_queue(
        settings.queue_name,
        durable=True,
        arguments={
            "x-max-priority": settings.max_priority,
            "x-dead-letter-exchange": settings.dlx_name,
            "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
        },
    )
    await work_queue.bind(exchange, routing_key=WORK_ROUTING_KEY)
    return Topology(exchange, dlx, work_queue, dlq)
