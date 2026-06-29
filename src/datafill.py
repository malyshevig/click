import random
import time
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client
from tqdm import tqdm

client = Client(host='localhost', port=9000, user='default', password='')

TOTAL_ROWS = 1_000_000_000
BATCH_SIZE = 500_000
NUM_THREADS = 4

# Справочники
CURRENCIES = ['USD', 'EUR', 'RUB', 'KZT']
STATUSES = ['success'] * 80 + ['failed'] * 10 + ['pending'] * 5 + ['refunded'] * 5
METHODS = ['card', 'sbp', 'wallet', 'crypto', 'bank_transfer']

# Шаблоны назначений платежей
PAYMENT_TEMPLATES = {
    'card': [
        'Оплата заказа №{order_id}',
        'Покупка в {shop_name}',
        'Оплата услуг {service_name}',
        'Подписка на {subscription}',
        'Покупка в магазине №{merchant_id}',
    ],
    'sbp': [
        'Перевод пользователю ID{user_id}',
        'Перевод по номеру телефона',
        'Возврат средств за заказ №{order_id}',
        'Перевод на счет {account_type}',
    ],
    'wallet': [
        'Пополнение электронного кошелька',
        'Перевод с карты на кошелек',
        'Вывод средств с кошелька',
        'Пополнение баланса для {service}',
    ],
    'crypto': [
        'Покупка криптовалюты (транзакция {tx_hash})',
        'Обмен {crypto_from} на {crypto_to}',
        'Перевод криптовалюты',
        'Покупка NFT #{nft_id}',
    ],
    'bank_transfer': [
        'Банковский перевод по договору №{contract_id}',
        'Оплата по счету №{invoice_id}',
        'Перевод на расчетный счет',
        'Погашение кредита по договору №{loan_id}',
        'Оплата коммунальных услуг за {month}',
    ]
}

# Справочники для шаблонов
SHOP_NAMES = [
    'Ozon', 'Wildberries', 'Яндекс.Маркет', 'М.Видео', 'DNS',
    'Lamoda', 'Спортмастер', 'Детский мир', 'Лента', 'Пятерочка',
    'Магнит', 'Перекресток', 'ВкусВилл', 'Ашан', 'Метро'
]

SERVICE_NAMES = [
    'такси', 'доставки еды', 'интернета', 'мобильной связи',
    'электричества', 'газа', 'воды', 'отопления', 'ТВ', 'Домофон'
]

SUBSCRIPTIONS = [
    'Яндекс.Плюс', 'Netflix', 'Spotify', 'YouTube Premium',
    'Apple Music', 'VK Музыка', 'Okko', 'Кинопоиск', 'IVI', 'More.tv'
]

ACCOUNT_TYPES = ['накопительный', 'текущий', 'валютный', 'инвестиционный', 'брокерский']

CRYPTO_NAMES = ['BTC', 'ETH', 'USDT', 'BNB', 'XRP', 'ADA', 'SOL', 'DOT', 'DOGE', 'MATIC']

MONTHS = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
          'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']


def generate_payment_purpose(method, merchant_id, user_id, amount):
    """Генерирует реалистичное назначение платежа"""
    template = random.choice(PAYMENT_TEMPLATES[method])

    # Заполняем плейсхолдеры в шаблоне
    replacements = {
        'order_id': random.randint(100000, 9999999),
        'shop_name': random.choice(SHOP_NAMES),
        'service_name': random.choice(SERVICE_NAMES),
        'subscription': random.choice(SUBSCRIPTIONS),
        'merchant_id': merchant_id,
        'user_id': user_id,
        'account_type': random.choice(ACCOUNT_TYPES),
        'service': random.choice(['игр', 'стриминга', 'хостинга', 'облачного хранилища']),
        'tx_hash': f'0x{random.getrandbits(64):016x}',
        'crypto_from': random.choice(CRYPTO_NAMES),
        'crypto_to': random.choice(CRYPTO_NAMES),
        'nft_id': random.randint(1, 999999),
        'contract_id': random.randint(10000, 999999),
        'invoice_id': random.randint(100000, 9999999),
        'loan_id': random.randint(10000, 999999),
        'month': random.choice(MONTHS),
    }

    # Заменяем все {ключи} на значения
    result = template
    for key, value in replacements.items():
        result = result.replace(f'{{{key}}}', str(value))

    return result


def generate_batch(batch_id):
    """Генерирует один батч данных с назначениями платежей"""
    data = []
    start_id = batch_id * BATCH_SIZE

    for i in range(BATCH_SIZE):
        payment_id = start_id + i
        created_at = int(time.time()) - random.randint(0, 94608000)
        user_id = random.randint(1, 10_000_000)
        merchant_id = random.randint(1, 50_000)
        amount = round(random.uniform(1.0, 50000.0), 2)
        method = random.choice(METHODS)

        # Генерируем назначение на основе метода и контекста
        payment_purpose = generate_payment_purpose(method, merchant_id, user_id, amount)

        data.append((
            payment_id,
            created_at,
            user_id,
            merchant_id,
            amount,
            random.choice(CURRENCIES),
            random.choice(STATUSES),
            method,
            payment_purpose
        ))
    return data


def insert_batch(batch_id):
    """Генерирует и вставляет батч"""
    try:
        data = generate_batch(batch_id)
        client.execute(
            'INSERT INTO payments (payment_id, created_at, user_id, merchant_id, amount, currency, status, payment_method, payment_purpose) VALUES',
            data,
            types_check=True
        )
        return True
    except Exception as e:
        print(f"Ошибка в батче {batch_id}: {e}")
        return False


if __name__ == '__main__':
    total_batches = TOTAL_ROWS // BATCH_SIZE

    print(f"Запуск генерации {TOTAL_ROWS:,} записей с назначениями платежей...")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = list(tqdm(
            executor.map(insert_batch, range(total_batches)),
            total=total_batches,
            desc="Загрузка данных"
        ))

    success_count = sum(1 for r in results if r)
    print(f"Готово! Успешно загружено батчей: {success_count}/{total_batches}")