# libraries imports here
import boto3
from botocore.exceptions import ClientError
import json
import gzip
import pandas as pd
from io import BytesIO

event = {"odate": "20230704"}
df_dict = {
        "place_id": [],
        "name": [],
        "lat": [],
        "lng": [],
        "business_status": [],
        "price_level": [],
        "rating": [],
        "user_ratings_total": [],
        "curbside_pickup": [],
        "dine_in": [],
        "delivery": [],
        "reservable": [],
        "serves_lunch": [],
        "serves_beer": [],
        "serves_breakfast": [],
        "serves_brunch": [],
        "serves_dinner": [],
        "serves_vegetarian_food": [],
        "serves_wine": [],
        "wheelchair_accessible_entrance": [],
        "url": [],
        "website": []
    }

def s3_get_partition_files(
        bucket,
        prefix,
        client
        ):
    print("get_partition_files")
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        print(response)
    except ClientError as e:
        print(e)
        return False
    return [obj["Key"] for obj in response["Contents"]]

def s3_get_file(
        bucket,
        key,
        client
        ):
    print("get_file", bucket, key)
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=key
        )
        print(response)
    except ClientError as e:
        print(e)
        return False
    return response

def get_nearby_json_values(file):
    print("get_nearby_json_values")
    dict_file = json.load(file)
    print(dict_file)
    for place in dict_file["results"]:
        for key in df_dict.keys():
            df_dict[key].append(place.get(key))
        df_dict["lat"].append(place["geometry"]["location"].get("lat"))
        df_dict["lng"].append(place["geometry"]["location"].get("lng"))

def s3_upload_file(
    bucket_name: str, 
    file_key: str,
    file_path: str
    ):
    """Upload a file to an S3 bucket

    Parameters:
        bucket_name: Bucket to upload to
        file_key: File key in the S3 bucket that the .csv will be uploaded
        file_path: temporary file in tmp/ directory
    Return:
        True if file was uploaded, else False
    """
    # Upload the file
    print("upload s3")
    try:
        s3_client = boto3.client("s3")
        response = s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=file_key
        )
        print(response)
    except ClientError as e:
        print(e)
        return False
    return True


# lambda_handler function
def lambda_handler(event, context):
    print(event)
    # Define the initial variables
    odate = event["odate"]
    bucket = "dcpgm-sor"
    destination_bucket = "dcpgm-sot"
    prefix = f"gmaps/nearby/{odate}/"
    s3_client = boto3.client("s3")

    # get all file names inside sor partition using the boto3
    all_filenames = s3_get_partition_files(
        bucket=bucket,
        prefix=prefix,
        client=s3_client
    )
    print(all_filenames)
    # iterate over all files using the boto3
    for file_name in all_filenames:
        print(file_name)

        object_response = s3_get_file(
            bucket=bucket,
            key=file_name,
            client=s3_client
        )
        object_body = object_response["Body"].read()
        with gzip.GzipFile(fileobj=BytesIO(object_body), mode='rb') as fh:
            get_nearby_json_values(file=fh)

    csv_path = "/tmp/nearby.csv"
    pd.DataFrame(df_dict).to_csv(path_or_buf=csv_path, index=False)

    csv_key = prefix+csv_path[5:]
    s3_upload_file(
        bucket_name=destination_bucket,
        file_key=csv_key,
        file_path=csv_path
    )