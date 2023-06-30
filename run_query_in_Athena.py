import boto3
import pandas as pd
import time

key = pd.read_csv("/Users/ha/Project/AWS/hahoang-local_accessKeys.csv")
YOUTUBE_API_KEY = key["Youtube Key"][0]
AWS_KEY_ID = key["Access key ID"][0]
AWS_SECRET = key["Secret access key"][0]
AWS_REGION = "ap-southeast-1"
DATABASE = "youtube"
TABLE_NAME = "youtube_videos"
BUCKET = "hh-youtube-data-storage"
RESULT_QUERY = "query-output"


def run_query():
    # Initialize Athena client
    athena_client = boto3.client(
        "athena",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY_ID,
        aws_secret_access_key=AWS_SECRET,
    )

    # Set the S3 output location for query results
    s3_output_location = f"s3://{BUCKET}/{RESULT_QUERY}/"
    # Vietnam, Philippines, Singapore, Thailand, Indonesia, Malaysia
    # create temporary table count number of videos each category
    # then asign rank based on num_videos for each country and date
    # finally get row has rank equal 1
    query = """
        WITH category_count AS (

        SELECT DATE(date_of_extraction) AS date_of_extraction, 
            CASE country 
                WHEN 'VN' THEN 'Vietnam' 
                WHEN 'PH' THEN 'Philippines'
                WHEN 'SG' THEN 'Singapore'
                WHEN 'TH' THEN 'Thailand'
                WHEN 'ID' THEN 'Indonesia'
                WHEN 'MY' THEN 'Malaysia' END AS country, category, 
            COUNT(*) AS num_videos
        FROM youtube_videos
        GROUP BY DATE(date_of_extraction), country,category),
        category_rank AS (
        SELECT date_of_extraction, country, category, num_videos,
                RANK() OVER(PARTITION BY  date_of_extraction, country ORDER BY num_videos DESC) AS rk
        FROM category_count)

        SELECT date_of_extraction,
            country, 
            category AS most_popular_category, num_videos
        FROM category_rank
        WHERE rk = 1
        ORDER BY date_of_extraction, country;
        """

    # Run the query in Athena
    query_execution = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    # Get the query execution ID
    query_execution_id = query_execution["QueryExecutionId"]
    time.sleep(10)

    # Get the query results
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract the results and save to a local file
    file_name = "most_popular_categories.csv"
    with open(file_name, "w", encoding="utf-8") as f:
        for row in query_results["ResultSet"]["Rows"]:
            f.write(",".join([data["VarCharValue"] for data in row["Data"]]) + "\n")

    # Upload the query results to S3
    s3_client = boto3.client(
        service_name="s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY_ID,
        aws_secret_access_key=AWS_SECRET,
    )
    s3_client.upload_file(file_name, BUCKET, f"{RESULT_QUERY}/{file_name}")

    # delete Athena's generated csv and metadata files
    s3_client.delete_object(Bucket=BUCKET, Key=f"query-output/{query_execution_id}.csv")
    s3_client.delete_object(
        Bucket=BUCKET, Key=f"query-output/{query_execution_id}.csv.metadata"
    )

    return None


if __name__ == "__main__":
    run_query()
