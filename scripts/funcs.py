from botocore.exceptions import ClientError
from datetime import date
import boto3
import json

TODAY = date.today().strftime("%Y-%m-%d")
YEAR = TODAY[:4]
MONTH = TODAY[5:7]
DAY = TODAY[8:10]
REGION_NAME = "eu-north-1"
BUCKET_NAME = "scrapy-data-bucket-872h5nh309ho4k"
INPUT_KEY = f'raw/{YEAR}/{MONTH}/{DAY}/jobs_{TODAY}.jsonl'
OUTPUT_KEY = f'processed/{YEAR}/{MONTH}/{DAY}/jobs_{TODAY}.jsonl'
DB_NAME = 'jobsdb'

def insert_job(job, conn):
    salary_data = job.get("salary")
    if salary_data:
        salary_min = salary_data.get("from")
        salary_max = salary_data.get("to")
        salary_currency = salary_data.get("currency")
    else:
        salary_min = None
        salary_max = None
        salary_currency = None

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


def get_api(region_name="eu-north-1"):

    secret_name = "Groq_API"

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
    api_key = secret_dict["api_key_gmail"]

    return api_key

def get_db_credentials(region_name="eu-north-1", db_name="jobsdb"):
    secret_name = "db_credentials"

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
    host = secret_dict["host"]
    user_name = secret_dict["username"]
    password = secret_dict["password"]
    port = secret_dict["port"]

    db_config = {
        "host": host,
        "port": port,
        "database": db_name,
        "user": user_name,
        "password": password
    }
    return db_config