import openai
import funcs as f
import psycopg2
import boto3
import json

processed_jobs = set()


s3 = boto3.client('s3')
client = openai.OpenAI()


db_config = f.get_db_credentials()

with psycopg2.connect(**db_config) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT NOW();")
        test_result = cur.fetchone()
        print("Connected. Current time:", test_result[0])
        
        response = s3.get_object(Bucket=f.BUCKET_NAME, Key=f.INPUT_KEY)

        lines = response['Body'].read().decode('utf-8').splitlines()

        data = [json.loads(line) for line in lines if line.strip()]
        processed_data = []
        print("Data loaded from S3 bucket: ", len(data))
        for row in data[:10]:
            if row.get("link") in processed_jobs:
                continue
            response = client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[
                    {"role": "system", "content": "You are a data transformation assistant. Your task is to output the transformed data as a single-line JSON object in English, fully standardized, with no extra text or explanation, ready to insert into a JSON file."},
                    {"role": "user", "content": f"Transform the given JSON data into a standardized JSON object with the fields: title, company, salary, category, location, languages, experience, employment_type, skills, link, publication_date, and description. All fields can be null if data is missing. Salary must be an object with keys “from”, “to”, and “currency”, or null if missing. Location must be a list of strings in English; if missing but remote is possible, use [“Remote”]. Languages must be an object with language names as keys and proficiency levels as values. Proficiency levels must be one of: elementary, pre-intermediate, intermediate, upper-intermediate, advanced, fluent. Experience must be an integer representing years of experience. Employment_type must be a string: Full-time, Part-time, or Internship. Skills must be a list of strings including skills inferred from the description if not explicitly listed. Category must be a string; if missing, infer the most relevant IT category based on the job description. Description must be a concise and precise string summary of the job. Fill missing values where possible using the description text. Here is the JSON data to transform: {row}"}
                ],
                max_tokens=300,
                temperature=0.25,
            )
            response_text = response.choices[0].message.content
            record = json.loads(response_text)

            if response_text:
                print("Transformed data: ", response_text)
                processed_data.append(response_text)
                f.insert_job(record, conn)
                conn.commit()
                processed_jobs.add(row.get("link"))

        if processed_data:
            s3.put_object(Bucket=f.BUCKET_NAME, Key=f.OUTPUT_KEY, Body='\n'.join(processed_data).encode('utf-8'))
        else:
            print("No data to process.")
