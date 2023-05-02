import json
import logging
import boto3
import os
from datetime import datetime, timedelta

# Set default values

bucket_name = ""
object_prefix = ""
cutoff_days = int(1)
batch_size = int(100)
s3_file_pattern = ""

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def get_env_variables():
  
    global bucket_name
    global object_prefix
    global cutoff_days
    global batch_size
    global s3_file_pattern
    
    print(f"\nTouch Files Parameters")
    print(f"======================\n")
    
    if os.getenv('BUCKET_NAME'):
        bucket_name = os.getenv('BUCKET_NAME')
        print(f"Touching files in bucket {bucket_name=}")
    else:
        print("Missing env varable BUCKET_NAME e.g. my-bucket")
        return None
      
    if os.getenv('OBJECT_PREFIX'):
        object_prefix = os.getenv('OBJECT_PREFIX')
        print(f"Touching files in folder {object_prefix=}")
    else:
        print("Missing env variable OBJECT_PREFIX e.g. /prd/dropoff")
        return None
      
    if os.getenv('CUTOFF_DAYS'):
        cutoff_days = int(os.getenv('CUTOFF_DAYS'))
        print(f"Touching with with a modified timestamp within the last {cutoff_days=}")
    else:
        print(f"Defaulting to touching with with a modified timestamp within the last {cutoff_days=}")
      
    if os.getenv('BATCH_SIZE'):
        batch_size = int(os.getenv('BATCH_SIZE'))
        print(f"Default to touching files in groups of {batch_size=}")
    else:
        print(f"Default to touching files in groups of {batch_size=}")
        
    if os.getenv('S3_FILE_PATTERN'):
        s3_file_pattern = os.getenv('S3_FILE_PATTERN')
        print(f"Default to touching files which meet the file pattern {s3_file_pattern=}")
    else:
        print(f"Default to selecting all files")

    return 1

def get_files_to_touch():
  
    try:
        s3_client = boto3.client('s3')
    except Exception as e:
        logger.error("Unable to initialize boto s3 client, supply correct AWS environment variables or set a AWS profile.")
        logger.error(str(e))

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=object_prefix, MaxKeys=batch_size)
    if 'Contents' not in resp:
        print("Nothing returned")
        return
    objs = resp["Contents"]

    cutoff = datetime.now() - timedelta(days=cutoff_days)


    # List files to be touched    
    for key in objs:
        lm = key["LastModified"].replace(tzinfo=None)

        if lm >= cutoff and key["StorageClass"] == "STANDARD" and (s3_file_pattern in str(key['Key']) or s3_file_pattern is None):
            print(f"Ready to touch file {key['Key']=}")
    input("\nPress Enter to continue, CTRL-C to cancel...\n")

    if 'NextContinuationToken' in resp:
        next_token = resp["NextContinuationToken"]
    else:
        next_token = ""
        
    while True:

        print("Processing batch with token: "+next_token)
        # Go through every item found and check it's "LastModified" date
        for key in objs:

            lm = key["LastModified"].replace(tzinfo=None)
            if lm >= cutoff and (s3_file_pattern in str(key['Key']) or s3_file_pattern is None):
                if key["StorageClass"] == "STANDARD":
                    print(f"Touching File {key['Key']=}")
                    
                    # Set Copy file
                    copy_source = {
                        'Bucket': bucket_name,
                        'Key': key["Key"]
                    }
                    s3_object = s3_resource.Object(bucket_name = bucket_name, key = key["Key"])
                    metadata_key = s3_client.head_object(Bucket = bucket_name, Key = key["Key"])
                    metadata = metadata_key['Metadata']
                    
                    # Save any ContentType Metadata if it exists
                    if metadata_key.get('ContentType'):
                        content_type = metadata_key['ContentType']
                    else:
                        content_type = ""

                    touch_timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                    metadata.update({'last_touched': touch_timestamp})

                    s3_object.copy_from(CopySource=copy_source,
                                        Metadata=metadata,
                                        MetadataDirective='REPLACE',
                                        ContentType=content_type)

                else:
                    print("Skipping "+key["Key"]+" - not STANDARD storage class")
        
        if next_token == "":
            break
        
        # Get next batch of objects            
        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=object_prefix, MaxKeys=batch_size, ContinuationToken=next_token)
        if 'Contents' not in resp:
            print("Nothing returned")
            return        
        objs = resp["Contents"]
        if 'NextContinuationToken' in resp:
            next_token = resp["NextContinuationToken"]
        else:
            next_token = ""

def main():
      
    env_variables_present = get_env_variables()
    
    if env_variables_present:
      
        input("\nPress Enter to continue, CTRL-C to cancel...\n")
        
        get_files_to_touch()

if __name__ == "__main__":
    main()

