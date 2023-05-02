# touch_s3_files
A small Python script to touch s3 files to update the last_modified timestamp

# Installation Instructions

To use the `touch_s3_files.py` script create the following requirements file

touch_s3_requirements.txt


```bash
boto3
```

Setting up a virtual environment to run the Python Program
```bash
# Make a Virtual Python Environment
python3.8 -m venv ~/.virtualenv/touch_files

# Activate the Virtual Python Environment
. ~/.virtualenv/touch_files/bin/activate

# Install required Python Packages
# Remember Create a file called touch_s3_requirements.txt

# Install the required packages
python3.8 -m pip install -r touch_s3_requirements.txt
```

# Run Instructions

The following bash script shows an example using the python script.

```bash
# Export the Environment Variables used by the Python Program
# Adjust variables to suit.
export BUCKET_NAME=<my bucket name>
export OBJECT_PREFIX=<my folder path e.g. prd/uploads>
export CUTOFF_DAYS=45
export BATCH_SIZE=100
export S3_FILE_PATTERN=<optional file pattern>

# Set AWS connection details so boto3 can connect to s3
export AWS_ACCESS_KEY_ID="<access_key_id>"
export AWS_SECRET_ACCESS_KEY="<secret_access_key>"
export AWS_SESSION_TOKEN="<session_token>"
export AWS_REGION="ap-southeast-2"

# Run the Python Program to update the modify dates.
python3.8 touch_s3_files.py
```
