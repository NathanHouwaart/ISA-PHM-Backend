from __future__ import annotations

import asyncio

import pytest

from app.concurrency import ConversionCapacityLimiter
from app.errors import APIError


def test_conversion_capacity_limiter_rejects_when_full() -> None:
    limiter = ConversionCapacityLimiter(1, acquire_timeout_seconds=0.001)

    async def exercise() -> APIError:
        async with limiter:
            with pytest.raises(APIError) as exc_info:
                async with limiter:
                    pass
            return exc_info.value

    error = asyncio.run(exercise())

    assert error.status_code == 429
    assert error.code == "conversion_capacity_exceeded"
    assert error.details == {"max_concurrent_conversions": 1}


def test_conversion_capacity_limiter_releases_slots() -> None:
    limiter = ConversionCapacityLimiter(1, acquire_timeout_seconds=0.001)

    async def exercise() -> None:
        async with limiter:
            pass
        async with limiter:
            pass

    asyncio.run(exercise())
