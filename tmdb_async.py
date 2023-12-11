# "threading is for working in parallel, and async is for waiting in parallel".

import time
import os
from dotenv import load_dotenv

import pandas as pd
from tqdm import tqdm
import asyncio
from time import perf_counter

import aiohttp

load_dotenv()
api_key = os.environ.get("API_KEY")

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {api_key}",
}


async def fetch(s, mid):
    async with s.get(f"https://api.themoviedb.org/3/movie/{mid}", headers=headers) as r:
        if r.status != 200:
            if r.status == 404:
                return {}
            r.raise_for_status()
        return await r.json()


async def fetch_all(s, mids):
    tasks = []
    for mid in mids:
        task = asyncio.create_task(fetch(s, mid))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    return res


async def main():
    mids = pd.read_json("movie_ids_12_01_2023.json", lines=True)
    mids = mids["id"].tolist()
    mids = sorted(mids)

    # Read all CSV filenames in data folder, and extract the index number to get the last index
    csv_filenames = os.listdir("data")
    csv_filenames = [
        filename for filename in csv_filenames if filename.endswith(".csv")
    ]
    csv_filenames = [filename.split(".")[0] for filename in csv_filenames]
    csv_filenames = [filename.split("-")[1] for filename in csv_filenames]
    csv_filenames = [int(filename) for filename in csv_filenames]
    from_idx = max(csv_filenames) + 1 if csv_filenames else 0
    print(f"Continuing from: {from_idx}")

    step = 100

    async with aiohttp.ClientSession() as session:
        for i in tqdm(range(from_idx, len(mids), step)):
            movies = await fetch_all(session, mids[i : i + step])
            movie_df = pd.DataFrame(movies)
            movie_df.to_csv(f"data/train_{i}-{i + step - 1}.csv", index=False)
            time.sleep(1)


if __name__ == "__main__":
    start = perf_counter()
    asyncio.run(main())
    stop = perf_counter()
    print("time taken:", stop - start)
