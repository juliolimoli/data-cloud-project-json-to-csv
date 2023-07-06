# libraries imports here
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
import json
import gzip
import pandas as pd

# receive event with odate
# get all file names inside sor partition using the boto3
# iterate over all files using the boto3
# gunzip the .gz
# create the pandas dataframe
# iterate over the json
# insert the values in the dataframe
# save the dataframe in .csv
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
        filename,
        client
        ):
    print("get_file", bucket, key, filename)
    try:
        response = client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=filename
        )
        print(response)
    except ClientError as e:
        print(e)
        return False
    return True

def clear_tmp_dir():
    print("Clearing the tmp/ dir")
    files = os.listdir('/tmp')
    for file_name in files:
        print("File clear:", file_name)
        file_path = os.path.join('/tmp', file_name)
        os.remove(file_path)

def gunzip_file(input_file, output_file):
    print("gunziping:", input_file, output_file)
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            f_out.writelines(f_in)

def get_nearby_json_values(file):
    print("get_nearby_json_values")
    with open(file) as f:
        dict_file = json.load(f)
        print(dict_file)
    for place in dict_file["results"]:
        for key in df_dict.keys():
            df_dict[key].append(place.get(key))

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
        input_file_path = "tmp/"+file_name

        clear_tmp_dir()

        s3_get_file(
            bucket=bucket,
            key=file_name,
            filename=input_file_path,
            client=s3_client
        )
        
        output_file_path = "tmp/"+file_name[:-2]+"json"

        gunzip_file(
            input_file=input_file_path, 
            output_file=output_file_path
        )

        get_nearby_json_values(file=output_file_path)

        csv_path = "tmp/nearby.csv"
        pd.DataFrame(df_dict).to_csv(csv_path)

        csv_key = prefix+csv_path[4:]
        s3_upload_file(
            bucket_name=destination_bucket,
            file_key=csv_key,
            file_path=csv_path
        )