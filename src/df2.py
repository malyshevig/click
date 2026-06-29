import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from clickhouse_driver import Client
from tqdm import tqdm

# Глобальный пул клиентов (по одному на поток)
_client_pool = threading.local()


def get_client():
    """Получить или создать клиент для текущего потока"""
    if not hasattr(_client_pool, 'client'):
        _client_pool.client = Client(
            host='gek',
            port=9000,
            user='default',
            password='click',
            settings={
                'max_insert_block_size': 1000000,
                'min_insert_block_size_rows': 1000000,
            }
        )
    return _client_pool.client


TOTAL_ROWS = 3_000_000_000
BATCH_SIZE = 1_000_000  # Увеличили до 1 млн (меньше накладных расходов)
NUM_THREADS = 8  # Увеличили до 8 потоков

# Справочники
CURRENCIES = ['USD', 'EUR', 'RUB', 'KZT']
STATUSES = ['success'] * 80 + ['failed'] * 10 + ['pending'] * 5 + ['refunded'] * 5
METHODS = ['card', 'sbp', 'wallet', 'crypto', 'bank_transfer']

PAYMENT_TEMPLATES = {
    'card': [
        'Оплата заказа №{order_id}',
        'Покупка в {shop_name}',
        'Оплата услуг {service_name}',
        'Подписка на {subscription}',
    ],
    'sbp': [
        'Перевод пользователю ID{user_id}',
        'Перевод по номеру телефона',
    ],
    'wallet': [
        'Пополнение электронного кошелька',
        'Перевод с карты на кошелек',
    ],
    'crypto': [
        'Покупка криптовалюты (транзакция {tx_hash})',
        'Обмен {crypto_from} на {crypto_to}',
    ],
    'bank_transfer': [
        'Банковский перевод по договору №{contract_id}',
        'Оплата по счету №{invoice_id}',
    ]
}

SHOP_NAMES = ['Ozon', 'Wildberries', 'Яндекс.Маркет', 'М.Видео', 'DNS', 'Lamoda']
SERVICE_NAMES = ['такси', 'доставки еды', 'интернета', 'мобильной связи']
SUBSCRIPTIONS = ['Яндекс.Плюс', 'Netflix', 'Spotify', 'YouTube Premium']
ACCOUNT_TYPES = ['накопительный', 'текущий', 'валютный']
CRYPTO_NAMES = ['BTC', 'ETH', 'USDT', 'BNB', 'XRP']


def generate_payment_purpose(method, merchant_id, user_id):
    """Генерирует назначение платежа"""
    template = random.choice(PAYMENT_TEMPLATES[method])

    replacements = {
        'order_id': random.randint(100000, 9999999),
        'shop_name': random.choice(SHOP_NAMES),
        'service_name': random.choice(SERVICE_NAMES),
        'subscription': random.choice(SUBSCRIPTIONS),
        'merchant_id': merchant_id,
        'user_id': user_id,
        'account_type': random.choice(ACCOUNT_TYPES),
        'tx_hash': f'0x{random.getrandbits(64):016x}',
        'crypto_from': random.choice(CRYPTO_NAMES),
        'crypto_to': random.choice(CRYPTO_NAMES),
        'contract_id': random.randint(10000, 999999),
        'invoice_id': random.randint(100000, 9999999),
    }

    result = template
    for key, value in replacements.items():
        result = result.replace(f'{{{key}}}', str(value))

    return result


def generate_batch(batch_id):
    """Генерирует один батч данных"""
    data = []
    start_id = batch_id * BATCH_SIZE

    for i in range(BATCH_SIZE):
        payment = make_payment(data, i, start_id)
        data.append(payment)
    return data


def make_payment(i: int, start_id: Any, pay_purpose:str=None):
    payment_id = start_id + i
    created_at = int(time.time()) - random.randint(0, 94608000)
    user_id = random.randint(1, 10_000_000)
    merchant_id = random.randint(1, 50_000)
    amount = round(random.uniform(1.0, 50000.0), 2)
    method = random.choice(METHODS)

    payment_purpose = pay_purpose if pay_purpose else generate_payment_purpose(method, merchant_id, user_id)

    return (
        payment_id,
        created_at,
        user_id,
        merchant_id,
        amount,
        random.choice(CURRENCIES),
        random.choice(STATUSES),
        method,
        payment_purpose
    )


def insert_batch(batch_id):
    """Генерирует и вставляет батч с retry-логикой"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = get_client()  # Получаем клиент для текущего потока
            data = generate_batch(batch_id)

            # types_check=False для ускорения
            client.execute(
                'INSERT INTO payment.payments (payment_id, created_at, user_id, merchant_id, amount, currency, status, payment_method, payment_purpose) VALUES',
                data,
                types_check=False
            )
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Экспоненциальная задержка
                continue
            print(f"Ошибка в батче {batch_id} (попытка {attempt + 1}): {e}")
            return False

    return False


def insert_one(pay_purpose):
    """Генерирует и вставляет батч с retry-логикой"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = get_client()  # Получаем клиент для текущего потока
            data = [make_payment(1,10000000, pay_purpose)]

            # types_check=False для ускорения
            client.execute(
                'INSERT INTO payment.payments (payment_id, created_at, user_id, merchant_id, amount, currency, status, payment_method, payment_purpose) VALUES',
                data,
                types_check=False
            )
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Экспоненциальная задержка
                continue
            print(f"Ошибка при добавлении (попытка {attempt + 1}): {e}")
            return False
    return False


def datafill_batch():
    total_batches = TOTAL_ROWS // BATCH_SIZE

    print(f"Запуск генерации {TOTAL_ROWS:,} записей в {total_batches} батчей...")
    print(f"Потоков: {NUM_THREADS}, размер батча: {BATCH_SIZE:,}")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = list(tqdm(
            executor.map(insert_batch, range(total_batches)),
            total=total_batches,
            desc="Загрузка данных"
        ))

    success_count = sum(1 for r in results if r)
    print(f"Готово! Успешно загружено батчей: {success_count}/{total_batches}")
    print(f"Всего записей: {success_count * BATCH_SIZE:,}")


if __name__ == '__main__':
    #datafill_batch()
    insert_one("Уникальный ключи")