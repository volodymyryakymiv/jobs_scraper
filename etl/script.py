import os
import json
from groq import Groq
import boto3
from botocore.exceptions import ClientError


def get_api():

    secret_name = "Groq_API"
    region_name = "eu-north-1"

    # The secret is stored in AWS Secrets ManagerBU

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)
    api_key = secret_dict["api_key"]

    return api_key


current_directory = os.getcwd()
s3 = boto3.client('s3')
BUCKET_NAME = "scrapy-data-bucket-872h5nh309ho4k"
INPUT_KEY = 'raw/data.jsonl'
OUTPUT_KEY = 'processed/data.jsonl'
client = Groq(api_key=get_api())

response = s3.get_object(Bucket=BUCKET_NAME, Key=INPUT_KEY)
lines = response['Body'].read().decode('utf-8').splitlines()

data = [json.loads(line) for line in lines if line.strip()]
processed_data = []
print("Data loaded from S3 bucket: ", len(data))
for row in data:
    chat_completion = client.chat.completions.create(
        messages = [
            {
                "role": "system",
                "content": "You are a data transformation assistant. Your task is to output the transformed data as JSON, without any other text."
            },
            {
                "role": "user",
                "content": f"""Transform the following JSON data into a structured format with the schema: 
                title, company, salary, category, location, languages, experience, employment_type, skills, link, publication_date and description. Values can be NULL.
                Constraints:
                - The salary must be a dictionary with keys "from", "to", and "currency".
                - The location must be a list of strings.
                - The languages must be a string or a dictionary with language names as keys and proficiency levels as values. The levels must be standardized to "pre-intermediate", "intermediate", "upper-intermediate", and "advanced".
                - The experience must be an integer representing the number of years.
                - The employment_type must be a list of strings.
                - The skills must be a list of strings.
                - The category must be a string. Get it from the "category" field. Or if it is not available, set it by yourself.
                - The link must be a string.
                - The publication_date must be a string in the format YYYY-MM-DD.
                - The description must be a string with short but precise text.
                Also you can fill null values if you can.
                The output must be a JSON object containing only the transformed data in English and standardized, without any additional text or explanation and ready for insertion into new JSON file.
                Here is the JSON data to transform:
                {row}
                """
            }
        ],
        model="llama-3.3-70b-versatile",
    )
    response_text = chat_completion.choices[0].message.content
    edited_text = response_text.replace("```json", '').replace("```", '')
    if len(edited_text.strip()):
        print("Transformed data: ", edited_text)
        processed_data.append(json.loads(edited_text))

if not len(processed_data):
    s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY, Body=processed_data.encode('utf-8'))
else:
    print("No data to process.")

