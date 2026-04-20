from fastapi import Request
from services.logger_service import ClickHouseLogger

def get_logger(request: Request) -> ClickHouseLogger:
    return request.app.state.logger