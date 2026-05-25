import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
import clickhouse_connect
from uuid import UUID

from clickhouse_connect.driver import AsyncClient

logger = logging.getLogger(__name__)

class ClickHouseLogger:
    def __init__(self, host: str, port: int, database: str, table: str,
                 user: str, password: str, buffer_size: int = 100, flush_interval: int = 5):
        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval

        self._client: Optional[AsyncClient] = None
        self._buffer: List[Dict[str, Any]] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def connect(self):
        self._client = await clickhouse_connect.get_async_client(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            autogenerate_session_id=False,
        )
        self._flush_task = asyncio.create_task(self._periodic_flush())

    async def close(self):
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self._flush_buffer()
        if self._client:
            await self._client.close()

    async def _periodic_flush(self):
        while True:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(e)

    async def _flush_buffer(self):
        async with self._lock:
            if not self._buffer:
                return
            try:
                column_names = list(self._buffer[0].keys())
                data_as_tuples = [
                    tuple(record[col] for col in column_names)
                    for record in self._buffer
                ]
                await self._client.insert(
                    table=self.table,
                    data=data_as_tuples,
                    column_names=column_names,
                    settings={
                        'async_insert': 1,
                        'wait_for_async_insert': 0,
                    }
                )
                logger.debug(f"Flushed {len(self._buffer)} log records to ClickHouse")
                self._buffer.clear()
            except Exception as e:
                logger.error(e)

    async def log_event(self, event_type: str, endpoint: str, http_method: str,
                        consumer_id: Optional[Union[UUID, str]] = None, item_id: Optional[Union[UUID, str]] = None,
                        category_id: Optional[Union[UUID, str]] = None, store_id: Optional[Union[UUID, str]] = None,
                        order_id: Optional[Union[UUID, str]] = None, price: Optional[float] = None,
                        name: Optional[str] = None, lastname: Optional[str] = None,
                        patronymic: Optional[str] = None, email: Optional[str] = None,
                        count_item: Optional[int] = None, error_message: Optional[str] = None,
                        status_code: Optional[int] = None, duration_ms: Optional[int] = None,
                        ):
        record = {
            "event_time": datetime.now(),
            "duration_ms": int(duration_ms) if duration_ms else None,
            "event_type": event_type,
            "consumer_id": str(consumer_id) if consumer_id else None,
            "endpoint": endpoint,
            "http_method": http_method,
            "item_id": str(item_id) if item_id else None,
            "category_id": str(category_id) if category_id else None,
            "store_id": str(store_id) if store_id else None,
            "order_id": str(order_id) if order_id else None,
            "price": float(price) if price is not None else None,
            "name": str(name) if name else None,
            "lastname": str(lastname) if lastname else None,
            "patronymic": str(patronymic) if patronymic else None,
            "email": str(email) if email else None,
            "count_item": count_item,
            "error_message": error_message,
            "status_code": status_code,
        }

        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self.buffer_size:
                asyncio.create_task(self._flush_buffer())

    def __call__(self, **kwargs):
        return self.log_event(**kwargs)