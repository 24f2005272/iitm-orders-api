import base64
import math
import time
import uuid
from collections import defaultdict, deque
from threading import Lock

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI()

TOTAL_ORDERS = 59
RATE_LIMIT = 18
WINDOW = 10.0

lock = Lock()
idempotency_store = {}
client_requests = defaultdict(deque)

catalog = [
    {"id": i, "product": f"Item-{i}", "quantity": 1}
    for i in range(1, TOTAL_ORDERS + 1)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    # Allow browser preflight requests
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get(
        "X-Client-Id",
        "default"
    )

    now = time.monotonic()

    with lock:
        bucket = client_requests[client_id]

        while bucket and now - bucket[0] >= WINDOW:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(
                1,
                math.ceil(WINDOW - (now - bucket[0]))
            )

            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)

    return await call_next(request)


class OrderInput(BaseModel):
    product: str = "Sample Product"
    quantity: int = 1


@app.post("/orders", status_code=201)
def create_order(
    order: OrderInput,
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key"
    ),
):
    if not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key required"
        )

    with lock:
        if idempotency_key in idempotency_store:
            return idempotency_store[idempotency_key]

        created = {
            "id": str(uuid.uuid4()),
            "product": order.product,
            "quantity": order.quantity,
        }

        idempotency_store[idempotency_key] = created

    return created


def encode_cursor(offset):
    raw = str(offset).encode()
    return base64.urlsafe_b64encode(
        raw
    ).decode().rstrip("=")


def decode_cursor(cursor):
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = int(
            base64.urlsafe_b64decode(
                padded
            ).decode()
        )
        if not 0 <= value <= TOTAL_ORDERS:
            raise ValueError()
        return value
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid cursor"
        )


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1),
    cursor: str | None = None,
):
    offset = decode_cursor(cursor) if cursor else 0

    page = catalog[offset:offset + limit]
    next_offset = offset + len(page)

    next_cursor = (
        encode_cursor(next_offset)
        if next_offset < TOTAL_ORDERS
        else None
    )

    return {
        "items": page,
        "next_cursor": next_cursor,
    }


@app.get("/healthz")
def health():
    return {"status": "ok"}
