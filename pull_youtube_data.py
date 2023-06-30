from googleapiclient.discovery import build
from datetime import date
import pandas as pd
import boto3
import io
import time

key = pd.read_csv("/Users/ha/Project/AWS/hahoang-local_accessKeys.csv")
YOUTUBE_API_KEY = key["Youtube Key"][0]
AWS_KEY_ID = key["Access key ID"][0]
AWS_SECRET = key["Secret access key"][0]
AWS_REGION = "ap-southeast-1"


def pull_data(region_code):
    api_service_name = "youtube"
    api_version = "v3"
    # create an API client
    youtube = build(api_service_name, api_version, developerKey=YOUTUBE_API_KEY)

    # make a request for the top videos
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        chart="mostPopular",
        regionCode=region_code,
        maxResults=50,
    )
    response = request.execute()

    # get current date
    today = date.today()

    # store response in a data frame
    df = pd.DataFrame(response["items"])
    df = pd.concat(
        [df.drop("snippet", axis=1), pd.DataFrame(df["snippet"].tolist())], axis=1
    )
    df = pd.concat(
        [df.drop("statistics", axis=1), pd.DataFrame(df["statistics"].tolist())], axis=1
    )

    # replace the category id with the name
    dict_category = {
        1: "Film & Animation",
        2: "Autos & Vehicles",
        10: "Music",
        15: "Pets & Animals",
        17: "Sports",
        18: "Short Movies",
        19: "Travel & Events",
        20: "Gaming",
        21: "Videoblogging",
        22: "People & Blogs",
        23: "Comedy",
        24: "Entertainment",
        25: "News & Politics",
        26: "Howto & Style",
        27: "Education",
        28: "Science & Technology",
        29: "Nonprofits & Activism",
        30: "Movies",
        31: "Anime/Animation",
        32: "Action/Adventure",
        33: "Classics",
        34: "Comedy",
        35: "Documentary",
        36: "Drama",
        37: "Family",
        38: "Foreign",
        39: "Horror",
        40: "Sci-Fi/Fantasy",
        41: "Thriller",
        42: "Shorts",
        43: "Shows",
        44: "Trailers",
    }
    df["categoryId"] = df["categoryId"].astype(int)
    df["category"] = df["categoryId"].map(dict_category)
    df.insert(0, "date_of_extraction", today)
    df.insert(1, "country", region_code)

    # columns to keep
    columns = [
        "date_of_extraction",
        "country",
        "id",
        "title",
        "description",
        "channelId",
        "channelTitle",
        "category",
        "viewCount",
        "likeCount",
        "favoriteCount",
        "commentCount",
    ]
    df = df[columns]
    df = df.dropna()

    int_col = df.columns[-4:]
    for col in int_col:
        df[col] = df[col].astype(int)
    str_col = df.columns[:-4]
    for col in str_col:
        df[col] = df[col].astype(str)

    df = df[
        [
            "date_of_extraction",
            "country",
            "id",
            "title",
            "channelId",
            "channelTitle",
            "category",
            "viewCount",
            "likeCount",
            "favoriteCount",
            "commentCount",
        ]
    ]

    # create a client
    s3 = boto3.client(
        service_name="s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY_ID,
        aws_secret_access_key=AWS_SECRET,
    )

    # upload pandas data frame to bucket
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, sep=",")
    s3.put_object(
        Bucket="hh-youtube-data-storage",
        Body=csv_buffer.getvalue(),
        Key=f"data/{region_code} videos {today}.csv",
    )

    return None


if __name__ == "__main__":
    # Vietnam, Philippines, Singapore, Thailand, Indonesia, Malaysia
    list_region = ["VN", "PH", "SG", "TH", "ID", "MY"]
    for re in list_region:
        pull_data(region_code=re)
