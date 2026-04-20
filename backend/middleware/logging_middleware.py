import traceback
import time
import asyncio
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response
from services.logger_service import ClickHouseLogger
from dependencies import get_logger


class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app,):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response: Response = None
        error_msg = None

        try:
            response = await call_next(request)
            return response
        except Exception as e:
            error_msg = str(e)
            traceback_str = traceback.format_exc()
            raise
        finally:
            logger = get_logger(request)
            asyncio.create_task(logger.log_event(
                event_type="http_request",
                endpoint=request.url.path,
                http_method=request.method,
                status_code=response.status_code if response else 500,
                duration_ms=round((time.time() - start_time) * 1000, 2),
                error_message=error_msg
            ))


