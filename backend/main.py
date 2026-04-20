from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

import config
from middleware.logging_middleware import LoggingMiddleware
from services.logger_service import ClickHouseLogger
from config import get_backend_port
from routers import consumer_router, category_router, item_router, cart_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.logger = ClickHouseLogger(
        host="clickhouse",
        port=config.get_clickhouse_http_port(),
        database=config.get_clickhouse_db(),
        user=config.get_logger_user(),
        password=config.get_logger_password(),
        table="logs",
        buffer_size=100,
        flush_interval=5,
    )
    await app.state.logger.connect()
    yield
    await app.state.logger.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(LoggingMiddleware)

app.include_router(consumer_router.router)
app.include_router(category_router.router)
app.include_router(item_router.router)
app.include_router(cart_router.router)

@app.get("/")
async def root():
    return {"message": "API started"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=get_backend_port())