from contextlib import contextmanager
from queue import Queue

from clickhouse_driver import Client

COLUMNS = (
    'payment_id',
    'created_at',
    'user_id',
    'merchant_id',
    'amount',
    'currency',
    'status',
    'payment_method',
    'payment_purpose',
)

CLIENT_KWARGS = {
    'host': 'gek',
    'port': 9000,
    'user': 'default',
    'password': 'click',
}

DEFAULT_POOL_SIZE = 10
ss
class ClickHouseConnectionPool:
    def __init__(self, pool_size: int = DEFAULT_POOL_SIZE, **client_kwargs):
        self._pool: Queue[Client] = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self._pool.put(Client(**client_kwargs))

    def acquire(self, timeout: float | None = None) -> Client:
        return self._pool.get(timeout=timeout)

    def release(self, client: Client) -> None:
        self._pool.put(client)

    def close_all(self) -> None:
        while not self._pool.empty():
            self._pool.get_nowait().disconnect()


_pool = ClickHouseConnectionPool(**CLIENT_KWARGS)


@contextmanager
def get_client():
    client = _pool.acquire()
    try:
        yield client
    finally:
        _pool.release(client)


def search_payments(payment: str, limit: int = 100) -> list[dict]:
    with get_client() as client:
        rows = client.execute(
            """
            SELECT payment_id, created_at, user_id, merchant_id, amount,
                   currency, status, payment_method, payment_purpose
            FROM payment.payments
            WHERE payment_purpose = %(payment)s
            LIMIT %(limit)s
            """,
            {'payment': payment, 'limit': limit},
        )
    return [dict(zip(COLUMNS, row)) for row in rows]


if __name__ == '__main__':
    for row in search_payments('Уникальный ключи'):
        print(row)
