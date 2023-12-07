import requests
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm

import concurrent.futures
import asyncio
import httpx


load_dotenv()
api_key = os.environ.get("API_KEY")

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {api_key}",
}


base_queries_done = 0
base_queries_todo = 0
new_df_data = []


def query_base_api(movie_id):
    response = requests.get(
        f"https://api.themoviedb.org/3/movie/{movie_id}", headers=headers
    )
    status_code = response.status_code
    response = response.json()
    print(response)
    global base_queries_done
    global new_df_data
    if status_code == 200:
        new_df_data += response
    else:
        new_df_data += None
    base_queries_done += 1
    if base_queries_done % 50 == 0:
        new_df = pd.DataFrame(new_df_data)
        new_df.to_csv("train_full.csv", index=False)
    return (status_code, response)


def get_base_data():
    movies = pd.read_json("movie_ids_12_01_2023.json", lines=True)
    base_queries_todo = len(movies)

    print(f"Total movies: {base_queries_todo}")
    movie_ids = movies["id"].unique()

    global new_df_data
    failed = []

    # Cut movies ids into chunks of 50
    chunks = [movie_ids[x : x + 50] for x in range(0, len(movie_ids), 50)]
    for chunk in tqdm(chunks):
        movie_ids = chunk
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for movie_id in movie_ids:
                futures.append(executor.submit(query_base_api, movie_id))
            for future in concurrent.futures.as_completed(futures):
                status_code, actors = future.result()

    print(f"Failed: {len(failed)}")
    print(f"Success: {len(new_df_data)}")

    # Save list of lists of dict to dataframe
    new_df_data = [item for sublist in new_df_data for item in sublist]
    new_df = pd.DataFrame(new_df_data)

    new_df.to_csv("train_full.csv", index=False)


finished = set()


def query_actor_api(movie_id):
    response = requests.get(
        f"https://api.themoviedb.org/3/movie/{movie_id}/credits", headers=headers
    )
    status_code = response.status_code
    response = response.json()
    actors = []
    if "cast" in response:
        cast = response["cast"]
        for actor in cast:
            actors.append(
                {
                    "imdb_id": movie_id,
                    "actor_id": actor["id"],
                    "actor_name": actor["name"],
                    "gender": actor["gender"],
                    "popularity": actor["popularity"],
                }
            )
        finished.add(movie_id)
        print(f"Finished: {len(finished)}")
        return (status_code, actors)
    return (status_code, response)


def get_actor_data():
    dirty = pd.read_csv("kaggle_train.csv")
    movie_ids = dirty["imdb_id"].unique()

    print(f"Total movies: {len(movie_ids)}")

    new_df_data = []
    failed = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for movie_id in movie_ids:
            futures.append(executor.submit(query_actor_api, movie_id))
        for future in concurrent.futures.as_completed(futures):
            status_code, actors = future.result()
            if status_code == 200:
                new_df_data.append(actors)
            else:
                failed.append(movie_id)

    print(f"Failed: {len(failed)}")
    print(f"Success: {len(new_df_data)}")

    # Save list of lists of dict to dataframe
    new_df_data = [item for sublist in new_df_data for item in sublist]
    new_df = pd.DataFrame(new_df_data)

    new_df.to_csv("train_actor.csv", index=False)


if __name__ == "__main__":
    get_base_data()
