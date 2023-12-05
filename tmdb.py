import requests
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm


load_dotenv()
api_key = os.environ.get("API_KEY")


def query_api(movie_id):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.get(
        f"https://api.themoviedb.org/3/movie/{movie_id}", headers=headers
    )
    print(f"{movie_id} - {response.status_code}")
    return (response.status_code, response.json())


if __name__ == "__main__":
    dirty = pd.read_csv("train.csv")
    clean = pd.read_csv("train_clean.csv")
    dirty_movie_ids = dirty["imdb_id"].unique()
    clean_movie_ids = clean["imdb_id"].unique()

    new_df_data = []

    movie_ids = set(dirty_movie_ids) - set(clean_movie_ids)
    # movie_ids = list(movie_ids)[:10]
    for movie_id in tqdm(movie_ids):
        status_code, row_data = query_api(movie_id)
        if status_code == 200:
            new_df_data.append(row_data)

    new_df = pd.DataFrame(new_df_data)
    clean = pd.concat([clean, new_df])
    clean.to_csv("train_clean.csv", index=False)
