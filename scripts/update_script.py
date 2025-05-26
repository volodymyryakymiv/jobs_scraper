import psycopg2
import requests
import funcs as f
from psycopg2.extras import execute_values

db_config = f.get_db_credentials()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
}

with psycopg2.connect(**db_config) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT NOW();")
        test_result = cur.fetchone()
        print("Connected. Current time:", test_result[0])
        
        active_jobs = cur.execute("SELECT j.id, j.link FROM jobs j WHERE j.is_active = TRUE;")
        active_jobs = cur.fetchall()
        inactive_jobs = set()
        for job_id, link in active_jobs:
            try:
                response = response = requests.get(link, headers=headers, timeout=5)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching job {job_id}: {e}")
                inactive_jobs.add((job_id,))
                continue
            if response.status_code != 200:
                print(f"Job {job_id} is no longer available. Marking as inactive.")
                inactive_jobs.add((job_id,))
            else:
                print(f"Job {job_id} is still active.")
        query = """
                    UPDATE jobs AS j
                    SET is_active = FALSE
                    FROM (VALUES %s) AS v(id)
                    WHERE v.id = j.id
                """
        execute_values(cur, query, inactive_jobs)
        conn.commit()