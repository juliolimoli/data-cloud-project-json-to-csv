# libraries imports here
import boto3
from botocore.exceptions import ClientError
import json
import gzip
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

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
    "administrative_area_level_1": [],
    "administrative_area_level_2": [],
    "administrative_area_level_3": [],
    "administrative_area_level_4": [],
    "administrative_area_level_5": [],
    "administrative_area_level_6": [],
    "administrative_area_level_7": [],
    "neighborhood": [],
    "locality": [],
    "sublocality_level_1": [],
    "sublocality_level_2": [],
    "sublocality_level_3": [],
    "sublocality_level_4": [],
    "sublocality_level_5": []
}
df_dict_opening_hours = {
    "place_id": [],
    "day_of_week": [],
    "open_hour": [],
    "close_hour": [],
    "insert_timestamp": [],
}
df_dict_types = {
    "place_id": [],
    "type": [],
    "insert_timestamp": []
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

def add_to_dicts(file):
    print("add values to dicts")
    dict_file = json.load(file)
    print(dict_file)
    place_id = dict_file["result"]["place_id"]
    entities = ["details", "address_components", "oppening_hours", "types"]
    insert_timestamp = datetime.now()
    # details
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
    # address components
    address_components = dict_file["result"].get("address_components")
    for key in df_dict_address_components:
        component_found = False
        for component in address_components:
            types = component["types"]
            if key in types:
                df_dict_address_components[key].append(
                component.get("long_name")
                )
                component_found = True
                break
        if not component_found and key != "place_id":
            df_dict_address_components[key].append(None)
    df_dict_address_components["place_id"].append(place_id)
    # opening hours
    try:
        periods = dict_file["result"].get("opening_hours").get("periods")
    except Exception as e:
        print(e)
    else:
        for period in periods:
            print(period)
            period_close = period.get("close")
            period_open = period.get("open")
            day = None
            time_close = None
            time_open = None
            if period_close is not None:
                day = period_close.get("day")
                time_close = period_close.get("time")
            if period_open is not None:
                day = period_open.get("day")
                time_open = period_open.get("time")
            df_dict_opening_hours["day_of_week"].append(day)
            df_dict_opening_hours["close_hour"].append(time_close)
            df_dict_opening_hours["open_hour"].append(time_open)      
            df_dict_opening_hours["insert_timestamp"].append(insert_timestamp)
            df_dict_opening_hours["place_id"].append(place_id)

def get_json_value(
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
            add_to_dicts(file=fh)

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
    prefix_root = f"gmaps/details/{odate}"
    s3_client = boto3.client("s3")

    # get all file names inside sor partition using the boto3
    all_filenames_details = s3_get_partition_files(
        bucket=bucket,
        prefix=prefix_root,
        client=s3_client
    )
    print(all_filenames_details)

    get_json_value(
        all_filenames=all_filenames_details,
        bucket=bucket,
        s3_client=s3_client
        )

    
    csv_path_details = "/tmp/details.csv.gz"
    csv_path_address_components = "/tmp/address_components.csv.gz"
    csv_path_opening_hours = "/tmp/opening_hours.csv.gz"
    csv_path_types = "/tmp/types.csv.gz"

    df_details = pd.DataFrame(df_dict_details)
    df_address_components = pd.DataFrame(df_dict_address_components)
    df_opening_hours = pd.DataFrame(df_dict_opening_hours)
    df_types = pd.DataFrame(df_dict_types)

    partition = f"year={odate[:4]}/month={odate[4:6]}/day={odate[6:8]}"

    df_details.to_csv(
        path_or_buf=csv_path_details, 
        index=False,
        compression='gzip'
        )
    df_address_components.to_csv(
        path_or_buf=csv_path_address_components,
        index=False,
        compression='gzip'
    )
    df_opening_hours.to_csv(
        path_or_buf=csv_path_opening_hours,
        index=False,
        compression='gzip'
    )
    df_types.to_csv(
        path_or_buf=csv_path_types,
        index=False,
        compression='gzip'
    )
    csv_key_details = f"gmaps/details/{partition}/details.csv.gz"
    csv_key_address_components = f"gmaps/address/{partition}/address.csv.gz"
    csv_key_hours = f"gmaps/hours/{partition}/hours.csv.gz"
    csv_key_types = f"gmaps/types/{partition}/types.csv.gz"

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
    s3_upload_file(
        bucket_name=destination_bucket,
        file_key=csv_key_hours,
        file_path=csv_path_opening_hours
    )
    s3_upload_file(
        bucket_name=destination_bucket,
        file_key=csv_key_types,
        file_path=csv_path_types
    )