import asyncio
import clickhouse_connect

async def main():
    client = await clickhouse_connect.get_async_client(host='gek', username="default", password="click")


    result = await client.query('create database if not exists payment')
    print(result.result_rows)
    await client.close()

asyncio.run(main())