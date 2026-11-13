"""Operational CLI: create API keys."""

from __future__ import annotations

import argparse
import asyncio

from jobplatform.config import get_settings
from jobplatform.db.session import create_engine, create_session_factory
from jobplatform.repo.api_keys import create_api_key


async def _create(args: argparse.Namespace) -> None:
    engine = create_engine(get_settings(), pool_size=1)
    factory = create_session_factory(engine)
    async with factory() as session, session.begin():
        raw, key_id = await create_api_key(
            session,
            owner=args.owner,
            name=args.name,
            rate_limit_capacity=args.rate_limit_capacity,
            rate_limit_refill_per_sec=args.rate_limit_refill,
        )
    await engine.dispose()
    print(raw)  # the only time the raw key is ever visible
    print(f"key id: {key_id}", file=__import__("sys").stderr)


def main() -> None:
    parser = argparse.ArgumentParser(prog="jobplatform.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create-api-key", help="create an API key and print it once")
    create.add_argument("--owner", required=True)
    create.add_argument("--name", default="default")
    create.add_argument("--rate-limit-capacity", type=int, default=None)
    create.add_argument("--rate-limit-refill", type=float, default=None)
    args = parser.parse_args()
    if args.command == "create-api-key":
        asyncio.run(_create(args))


if __name__ == "__main__":
    main()
