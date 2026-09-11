from __future__ import annotations

import asyncio

from app.errors import APIError


class ConversionCapacityLimiter:
    def __init__(self, capacity: int, acquire_timeout_seconds: float = 0.05) -> None:
        self.capacity = max(1, capacity)
        self.acquire_timeout_seconds = max(0.001, acquire_timeout_seconds)
        self._semaphore = asyncio.Semaphore(self.capacity)

    async def __aenter__(self) -> "ConversionCapacityLimiter":
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self.acquire_timeout_seconds,
            )
        except TimeoutError as exc:
            raise APIError(
                429,
                "conversion_capacity_exceeded",
                "The conversion service is busy. Please try again shortly.",
                {"max_concurrent_conversions": self.capacity},
            ) from exc
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        self._semaphore.release()
