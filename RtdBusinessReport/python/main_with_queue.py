import asyncio
import os

from aiofile import async_open
import clickhouse_connect

from transform_v2 import main_process
folder_path = "/root/cc_ch"
all_files = [
    os.path.join(folder_path, f)
    for f in os.listdir(folder_path)
    if os.path.isfile(os.path.join(folder_path, f))
]

PROCESS_QUEUE = asyncio.Queue()
CH_QUEUE = asyncio.Queue()
PROCESSOR_COUNT = 7  # parallel processing workers

async def read_file(file_path: str):
    """Read file asynchronously and return list of lines."""

    async with async_open(file_path, mode='r',) as f:
        data = await f.read()
        if data:
            await PROCESS_QUEUE.put((file_path, data))
    print(f"Read {file_path}")


async def file_reader():
    tasks = [asyncio.create_task(read_file(file_path)) for file_path in all_files]
    await asyncio.gather(*tasks)

    for _ in range(PROCESSOR_COUNT):
        await PROCESS_QUEUE.put((-1, -1))

async def processor():
    while True:
        file_path,records = await PROCESS_QUEUE.get()
        if  records == -1:
            await CH_QUEUE.put((-1,-1))
            PROCESS_QUEUE.task_done()
            break

        processed_data = await asyncio.to_thread(main_process,file_path,records)  # example processing
        await CH_QUEUE.put((file_path,processed_data))
        PROCESS_QUEUE.task_done()


async def clickhouse_loader():
    client = await clickhouse_connect.get_async_client(
        host='192.168.5.197',
        port=8123,
        username='default',
        password='123123',
        database="rtd",
        secure=False
    )
    sentinels_received=0
    while True:
        file_name, processed_data = await CH_QUEUE.get()
        if processed_data == -1:
            sentinels_received += 1
            if sentinels_received == PROCESSOR_COUNT:
                CH_QUEUE.task_done()
                break
            CH_QUEUE.task_done()
            continue
        await client.insert("hamid_processed_data",processed_data, column_names=['file_name', 'msisdn', 'timestamp', 'cat_list', 'p_dis_list', 'trigger_list'])
        CH_QUEUE.task_done()
        print(f"Inserted:",file_name)

async def main():
    tasks = [
        asyncio.create_task(file_reader()),
        *[asyncio.create_task(processor()) for _ in range(PROCESSOR_COUNT)],
        asyncio.create_task(clickhouse_loader())
    ]
    await asyncio.gather(*tasks)

asyncio.run(main())
