# libraries imports here
import boto3
from botocore.exceptions import ClientError
import json
import gzip
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

df_dict_nearby = {
    "place_id": [],
    "name": [],
    "lat": [],
    "lng": [],
    "business_status": [],
    "price_level": [],
    "rating": [],
    "user_ratings_total": []
}
df_dict_details = {
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
df_dict_address_components = {
    "place_id": [],
    "country": [],
    "state": [],
    "city": [],
    "neighborhood": [],
}

def set_odate(event):
    odate = event.get("odate")
    if odate is None:
        timestamp = datetime.now()
        previous_day = timestamp - timedelta(days=1)
        odate = previous_day.strftime("%Y%m%d")
        return odate
    else:
        return odate

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
        for key in df_dict_nearby.keys():
            print(place, key)
            if key not in ["lat", "lng"]:
                df_dict_nearby[key].append(place.get(key))
            elif key == "lat":
                df_dict_nearby["lat"].append(
                    place["geometry"]["location"].get("lat")
                )
            elif key == "lng":
                df_dict_nearby["lng"].append(
                    place["geometry"]["location"].get("lng")
                )

def get_details_json_values(file):
    print("get_details_json_values")
    dict_file = json.load(file)
    print(dict_file)
    for key in df_dict_details.keys():
        print(key)
        if key not in ["lat", "lng"]:
            df_dict_details[key].append(dict_file["result"].get(key))
        elif key == "lat":
            df_dict_details[key].append(
                dict_file["result"]["geometry"]["location"].get(key)
            )
        elif key == "lng":
            df_dict_details[key].append(
                dict_file["result"]["geometry"]["location"].get(key)
            )
    df_dict_address_components["place_id"].append(
        dict_file["result"]["place_id"].get("place_id")
        )
    address_components = dict_file["result"].get("address_components")
    for component in address_components:
        types = address_components["types"]
        if "country" in types:
            df_dict_address_components["country"].append(
                component.get("long_name")
            )
        elif "administrative_area_level_1" in types:
            df_dict_address_components["state"].append(
                component.get("long_name")
            )
        elif "administrative_area_level_2" in types:
            df_dict_address_components["city"].append(
                component.get("long_name")
            )
        elif "sublocality_level_1" in types:
            df_dict_address_components["neighborhood"].append(
                component.get("long_name")
            )

def add_to_dicts(
        nearby_or_details,
        all_filenames,
        bucket,
        s3_client
        ):
    for file_name in all_filenames:
        print(file_name)
        object_response = s3_get_file(
            bucket=bucket,
            key=file_name,
            client=s3_client
        )
        object_body = object_response["Body"].read()
        with gzip.GzipFile(fileobj=BytesIO(object_body), mode='rb') as fh:
            if nearby_or_details == "nearby":
                get_nearby_json_values(file=fh)
            elif nearby_or_details == "details":
                get_details_json_values(file=fh)

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
    # Define the initial variables
    odate = set_odate(event=event)
    bucket = "dcpgm-sor"
    destination_bucket = "dcpgm-sot"
    prefix = f"gmaps/nearby/{odate}/"
    prefix_details = f"gmaps/details/{odate}/"
    s3_client = boto3.client("s3")

    # get all file names inside sor partition using the boto3
    all_filenames_nearby = s3_get_partition_files(
        bucket=bucket,
        prefix=prefix,
        client=s3_client
    )
    print(all_filenames_nearby)

    all_filenames_details = s3_get_partition_files(
        bucket=bucket,
        prefix=prefix_details,
        client=s3_client
    )
    print(all_filenames_details)

    add_to_dicts(
        nearby_or_details="nearby",
        all_filenames=all_filenames_nearby,
        bucket=bucket,
        s3_client=s3_client
        )
    add_to_dicts(
        nearby_or_details="details",
        all_filenames=all_filenames_details,
        bucket=bucket,
        s3_client=s3_client
        )

    csv_path_nearby = "/tmp/nearby.csv"
    csv_path_details = "/tmp/details.csv"
    csv_path_address_components = "/tmp/address_components.csv"

    df_nearby = pd.DataFrame(df_dict_nearby)
    df_details = pd.DataFrame(df_dict_details)
    df_address_components = pd.DataFrame(df_dict_address_components)

    df_nearby.to_csv(path_or_buf=csv_path_nearby, index=False)
    df_details.to_csv(path_or_buf=csv_path_details, index=False)
    df_address_components.to_csv(path_or_buf=csv_path_details, index=False)
    
    country = df_address_components["country"].value_counts().index[0]
    state = df_address_components["state"].value_counts().index[0]
    city = df_address_components["city"].value_counts().index[0]
    partition = f"country={country}/state={state}/city={city}"

    csv_key_nearby = f"gmaps/nearby/{partition}/nearby.csv"
    csv_key_details = f"gmaps/details/{partition}/details.csv"
    csv_key_address_components = f"gmaps/address/{partition}/address.csv"

    s3_upload_file(
        bucket_name=destination_bucket,
        file_key=csv_key_nearby,
        file_path=csv_path_nearby
    )
    s3_upload_file(
        bucket_name=destination_bucket,
        file_key=csv_key_details,
        file_path=csv_path_details
    )
    s3_upload_file(
        bucket_name=destination_bucket,
        file_key=csv_key_address_components,
        file_path=csv_path_address_components
    )