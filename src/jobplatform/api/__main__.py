import uvicorn

from jobplatform.api.app import create_app
from jobplatform.config import get_settings

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,
        access_log=False,
        timeout_graceful_shutdown=20,
    )
