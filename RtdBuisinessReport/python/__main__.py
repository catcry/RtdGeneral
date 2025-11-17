import asyncio
import os
import traceback

from datetime import datetime
from multiprocessing import Pool, cpu_count

from aiofile import async_open
import clickhouse_connect


def get_field(field_name: str, data: str) -> str:
    field_idx = data.find(field_name)
    end_of_line_idx = data.find("\n", field_idx)
    return data[field_idx + len(field_name):end_of_line_idx].strip()


def get_simple_block(block_name: str, data: str) -> list[str]:
    block_idx = data.find(block_name)
    end_of_block_idx = data.find(".", block_idx)
    ids = [line.split(' ', 2)[1] for line in data[block_idx:end_of_block_idx].strip().splitlines() if
           line.startswith('F ')]
    if 'REASON' in ids:
        print(data)
    return ids


def get_action_monitor(am_data: str) -> list[str]:
    ids = []
    pos = 0

    while True:
        # Find the next logic block
        logic_start = am_data.find('B LOGIC', pos)
        if logic_start == -1:
            break

        # Find the end of this logic block (first '.' after logic_start)
        logic_end = am_data.find('.', logic_start)
        if logic_end == -1:
            break

        # Look for the ID line within this block
        id_start = am_data.find('F ID ', logic_start, logic_end)
        if id_start != -1:
            id_end = am_data.find('\n', id_start, logic_end)
            if id_end == -1:
                id_end = logic_end
            id_value = am_data[id_start + len('F ID '):id_end].strip()
            ids.append(id_value)

        # Move to the character after the current block
        pos = logic_end + 1

    return ids


def parse_block(file_name: str, data: str) -> tuple[str, str, datetime, list[str], list[str], list[str]]|tuple:
    cat_list: list[str] = []
    p_dist_list: list[str] = []
    trigger_list: list[str] = []

    if 'callingNumber' not in data or 'eventStartTimestamp' not in data:
        return tuple()
    msisdn = get_field("callingNumber", data)
    timestamp = datetime.strptime(get_field("eventStartTimestamp", data), "%Y%m%d%H%M%S")

    category_blok_idx = data.find('B __CATEGORIZATION__')
    rule_engine_blok_idx = data.find('B __RULE_ENGINE__')
    action_monitor_blok_idx = data.find('B __ACTION_MONITOR__')

    if category_blok_idx != -1:
        cat_list = get_simple_block('B FAILED_LOGICS', data[
            category_blok_idx:min((i for i in (rule_engine_blok_idx, action_monitor_blok_idx, len(data)) if i != -1))])

    if rule_engine_blok_idx != -1:
        trigger_list = get_simple_block('B TRIGGERED_ACTIONS', data[
            rule_engine_blok_idx:min((i for i in (action_monitor_blok_idx, len(data)) if i != -1))])

    if action_monitor_blok_idx != -1:
        p_dist_list = get_action_monitor(data[rule_engine_blok_idx:])

    return file_name, msisdn, timestamp, cat_list, p_dist_list, trigger_list


def parse_records(file_name, data: str) -> list[tuple]:
    RECORD_TAG = 'RECORD'
    previous_record_idx = data.find(RECORD_TAG)
    # print('RecordCount',data.count(RECORD_TAG))
    total_records = []
    while True:
        next_record_idx = data.find(RECORD_TAG, previous_record_idx + 1)
        if next_record_idx == -1:
            processed_records = parse_block(file_name, data[previous_record_idx:])
            if processed_records:
                total_records.append(processed_records)
            break

        processed_records = parse_block(file_name, data[previous_record_idx:next_record_idx])
        if processed_records:
            total_records.append(processed_records)
        previous_record_idx = next_record_idx
    return total_records


def main_process(file_path, data):
    # file_path, records = result
    file_name = os.path.basename(file_path)
    try:
        records = parse_records(file_name, data)
        return records
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        traceback.print_exc()


async def read_file(file_path):
    async with async_open(file_path, mode='r') as f:
        data = await f.read()
        # records = data.split('\n')
        # print(file_path)
        return file_path, data


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

    for i in range(0, len(all_files), BATCH_SIZE):
        print(f'Inserting [{i + 1}-{min(len(all_files), i + BATCH_SIZE)}/{len(all_files)}] files.')
        await batch(all_files[i:i + BATCH_SIZE])


if __name__ == '__main__':
    asyncio.run(main())
