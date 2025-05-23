import os
import json
import psycopg2
from groq import Groq
import boto3
from botocore.exceptions import ClientError

REGION_NAME = "eu-north-1"
BUCKET_NAME = "scrapy-data-bucket-872h5nh309ho4k"
INPUT_KEY = 'raw/data.jsonl'
OUTPUT_KEY = 'processed/data.jsonl'
DB_NAME = 'jobs'

def insert_job(job, conn):

    salary_data = job.get("salary", {})
    salary_min = salary_data.get("from")
    salary_max = salary_data.get("to")
    salary_currency = salary_data.get("currency")
    locations = job.get("locations", [])
    job['locations'] = locations if locations else "Remote"

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs (
                title, company, salary_min, salary_max, salary_currency,
                category, experience, description, link, publication_date, employment_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (link) DO NOTHING
            RETURNING id
        """, (
            job['title'],
            job['company'],
            salary_min,
            salary_max,
            salary_currency,
            job['category'],
            job['experience'],
            job['description'],
            job['link'],
            job['publication_date'],
            job['employment_type']
        ))

        job_id = cur.fetchone()
        if not job_id:
            print(f"Job already exists: {job['link']}")
            return
        job_id = job_id[0]

        # 2. Locations
        for loc in job['location']:
            cur.execute("INSERT INTO locations (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", (loc,))
            cur.execute("SELECT id FROM locations WHERE name = %s", (loc,))
            loc_id = cur.fetchone()[0]
            cur.execute("INSERT INTO job_locations (job_id, location_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (job_id, loc_id))

        # 3. Skills
        for skill in job['skills']:
            cur.execute("INSERT INTO skills (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", (skill,))
            cur.execute("SELECT id FROM skills WHERE name = %s", (skill,))
            skill_id = cur.fetchone()[0]
            cur.execute("INSERT INTO job_skills (job_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (job_id, skill_id))

        # 4. Languages
        for lang, level in job['languages'].items():
            cur.execute("INSERT INTO languages (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", (lang,))
            cur.execute("SELECT id FROM languages WHERE name = %s", (lang,))
            lang_id = cur.fetchone()[0]
            cur.execute("INSERT INTO job_languages (job_id, language_id, level) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (job_id, lang_id, level))


def get_api():

    secret_name = "Groq_API"

    # The secret is stored in AWS Secrets ManagerBU

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION_NAME
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

def get_db_credentials():
    secret_name = "db_credentials"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION_NAME
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)
    host = secret_dict["host"]
    user_name = secret_dict["username"]
    password = secret_dict["password"]
    port = secret_dict["port"]

    db_config = {
        "host": host,
        "port": port,
        "database": DB_NAME,
        "user": user_name,
        "password": password
    }
    return db_config


current_directory = os.getcwd()
s3 = boto3.client('s3')
client = Groq(api_key=get_api())

db_config = get_db_credentials()

with psycopg2.connect(**db_config) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT NOW();")
        test_result = cur.fetchone()
        print("Connected. Current time:", test_result[0])
        
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
                        "content": f"Transform the given JSON data into a standardized JSON object with the fields: title, company, salary, category, location, languages, experience, employment_type, skills, link, publication_date, and description. All fields can be null if data is missing. Salary must be an object with keys “from”, “to”, and “currency”, or null if missing. Location must be a list of strings; if missing but remote is possible, use [“Remote”]. Languages must be an object with language names as keys and proficiency levels as values. Proficiency levels must be one of: elementary, pre-intermediate, intermediate, upper-intermediate, advanced, fluent. Experience must be an integer representing years of experience. Employment_type must be a string: Full-time, Part-time, or Internship. Skills must be a list of strings including skills inferred from the description if not explicitly listed. Category must be a string; if missing, infer the most relevant category based on the job description. Description must be a concise and precise string summary of the job. Fill missing values where possible using the description text. Output the transformed data as a single-line JSON object in English, fully standardized, with no extra text or explanation, ready to insert into a JSON file. Here is the JSON data to transform: {row}"
                    }
                ],
                model="llama-3.3-70b-versatile",
            )
            response_text = chat_completion.choices[0].message.content
            edited_text = response_text.replace("```json", '').replace("```", '').strip()
            record = json.loads(edited_text)

            if len(edited_text):
                print("Transformed data: ", edited_text)
                processed_data.append(edited_text)

        if len(processed_data):
            s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY, Body='\n'.join(processed_data).encode('utf-8'))
        else:
            print("No data to process.")
