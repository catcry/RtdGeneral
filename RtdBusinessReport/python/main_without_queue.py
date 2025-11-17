import asyncio
import os
from multiprocessing import Pool,cpu_count
from aiofile import async_open
import clickhouse_connect

from transform_v2 import main_process


async def read_file(file_path):
    async with async_open(file_path, mode='r') as f:
        data = await f.read()
        # records = data.split('\n')
        # print(file_path)
        return file_path ,data
async def batch(batch_files):

    """Read file asynchronously and return list of lines."""

    client = await clickhouse_connect.get_async_client(
        host='192.168.5.197',
        port=8123,
        username='default',
        password='123123',
        database="rtd",
        secure=False
    )
    print('reading files...', end='')
    result = await asyncio.gather(*[asyncio.create_task(read_file(file_path)) for file_path in batch_files])
    print('done.')
    print('processing...', end='')


    # Create a single process pool (reuse for all tasks)

    with Pool(cpu_count()) as pool:

        processed_results = pool.starmap(main_process, result)
    processed_data = [record for list_of_records in processed_results for record in list_of_records]
    print('done.')

    print(f'Inserting {len(processed_data)} records in clickhouse ...', end='')

    await client.insert(
        "hamid_processed_data", processed_data,
        column_names=['file_name', 'msisdn', 'timestamp', 'cat_list', 'p_dis_list', 'trigger_list']

    )
    print('done.')



async def main():
    BATCH_SIZE = 100
    folder_path = "/root/cc_ch"
    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    for i in range(0,len(all_files),BATCH_SIZE):
        print(f'Inserting [{i+1}-{min(len(all_files),i+BATCH_SIZE)}/{len(all_files)}] files.')
        await batch(all_files[i:i+BATCH_SIZE])

if __name__ == '__main__':
    asyncio.run(main())
