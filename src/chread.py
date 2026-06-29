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

_client = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = Client(
            host='gek',
            port=9000,
            user='default',
            password='click',
        )
    return _client


def search_payments(payment: str, limit: int = 100) -> list[dict]:
    client = get_client()
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
