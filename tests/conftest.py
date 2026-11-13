"""Shared test configuration. Points testcontainers at Docker Desktop when needed."""

from __future__ import annotations

import os
from pathlib import Path

if "DOCKER_HOST" not in os.environ and not Path("/var/run/docker.sock").exists():
    _sock = Path.home() / ".docker" / "run" / "docker.sock"
    if _sock.exists():
        os.environ["DOCKER_HOST"] = f"unix://{_sock}"
        os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")
