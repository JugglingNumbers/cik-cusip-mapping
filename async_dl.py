import argparse
import csv
import os
import asyncio
from pathlib import Path
import time
import pandas as pd
import aiohttp

async def dequeue(sem, sleep):
    """Wait for a duration and then increment the semaphore"""
    try:
        await asyncio.sleep(sleep)
    finally:
        sem.release()


async def task(sem, sleep, row, folder, user_agent, n, len_):
    """Decrement the semaphore, schedule an increment, and then work"""
    await sem.acquire()
    
    # logic here
    cik = str(row[0]).strip()
    date = row[-2].strip()
    year = row[-2].split("-")[0].strip()
    month = row[-2].split("-")[1].strip()
    url = row[-1].strip()
    Path(f"./{folder}/{year}_{month}").mkdir(parents=True, exist_ok=True)
    if os.path.exists(f"./{folder}/{year}_{month}/{cik}_{date}.txt"):
        sem.release()
        return
        
    asyncio.create_task(dequeue(sem, sleep))
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://www.sec.gov/Archives/{url}", headers=user_agent, timeout=60
            ) as response:
                text = await response.text()
                if response.status != 200:
                    print(f"{cik}, {date} failed to download")
                    return
                with open(f"./{folder}/{year}_{month}/{cik}_{date}.txt", "w", errors="ignore") as f:
                    f.write(text)
    except Exception as e:
        print(f"{cik}, {date} failed to download. Error: {e}")
    print(f"{n} of {len_} requests completed")

async def main(folder, user_agent, to_dl):
    max_concurrent = 10
    sleep = 1.2
    sem = asyncio.Semaphore(max_concurrent)
    tasks = [asyncio.create_task(task(sem, sleep, row, folder, user_agent, n, len(to_dl))) for n, row in enumerate(to_dl)]
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filing", type=str)
    parser.add_argument("folder", type=str)
    parser.add_argument("offset_in_to_dl", type=int)

    user_agent = {"User-agent": "Mozilla/5.0"}

    args = parser.parse_args()
    filing = args.filing
    folder = args.folder
    offset = args.offset_in_to_dl

    to_dl = []
    count = 0
    if os.path.exists('to_dl.csv'):
        to_dl = [list(row) for row in pd.read_csv('to_dl.csv').values]
    else:
        with open("full_index.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                #count+=1
                if filing in row["form"]:
                    to_dl.append(row)
                if count == 1000:
                    break
        to_dl = pd.DataFrame(to_dl)
        to_dl.to_csv('to_dl.csv',index=False)
        to_dl = [list(row) for row in to_dl.values]

    len_ = len(to_dl)
    print(len_)
    print("start to download")
    
    start = time.time()
    asyncio.run(main(folder, user_agent, to_dl[offset:]))
    print(f"requests took {(time.time() - start)/60}")