import requests
from bs4 import BeautifulSoup
import time
import psycopg2
import re
from datetime import datetime, timedelta
import configparser
from typing import Tuple

# 🔹 Configurable Information
config = configparser.ConfigParser()
config.read('config.ini')

DB_CONFIG = config['Database']
JOB_SETTING = config['Job_Setting']

def test_connection(DB_CONFIG) -> Tuple[bool,str]:
    '''
    This function test connect to DB.
    
    Returns:
    
    (bool,str): If success -> (True, PostgreSQL version), Fail -> (False, diagnostic error from psycopg2.OperationalError)
    
    '''
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        conn.close()
        
        return (True, db_version[0].split('(')[0].strip())
    except Exception as err:
        return (False, err)
    
def test_table(DB_CONFIG) -> bool:
    '''
    This function test is exists linkedin_data_raw table or not
    
    Returns:
    
    True or False: return True if exist table and type of all columns is true or False otherwise.
    '''
    
    table_name = 'linkedin_data_raw'
    schemaname = 'public'
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Query to check if the table exists
    check_query = """
    SELECT EXISTS (
        SELECT 1 
        FROM pg_catalog.pg_tables 
        WHERE schemaname = %s
        AND tablename = %s
    );
    """
    
    cursor.execute(check_query, (
        schemaname,
        table_name
    ))

    conn.commit()
    
    # Fetch the result
    exists = cursor.fetchone()[0]
    
    cursor.close()
    
    # Output whether the table exists or not
    if exists:
        # Create a cursor to interact with the database
        cursor = conn.cursor()

        # Query to get column names and data types
        cursor.execute(f'''
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s;
        ''', (table_name,))

        # Fetch all rows of the result
        columns = cursor.fetchall()
        
        true_data_type = dict()
        true_data_type['id'] = 'integer'
        true_data_type['job_id'] = 'bigint'
        true_data_type['time_posted'] = 'timestamp without time zone'
        true_data_type['num_applicants'] = 'integer'
        true_data_type['process_date'] = 'timestamp without time zone'
        true_data_type['description'] = 'text'
        true_data_type['job_title'] = 'text'
        true_data_type['company_name'] = 'text'
        true_data_type['location'] = 'text'

        # Print the column names and their data types
        if columns:
            for column in columns:
                column_name, data_type = column
                if true_data_type[column_name] != data_type:
                    print(f"Column: {column_name} invalid type, expect: {true_data_type[column_name]} but got {data_type}")
        else:
            print(f"Table {table_name} does not exist or has no columns.")
            
        return True
    else:
        return False
    
    
    
    
    

# 🔹 Function to convert time_posted string into a datetime object
def parse_time_posted(time_str):
    """
    Converts a time string (e.g., "1 day ago", "2 weeks ago", "3 hours ago")
    into the corresponding datetime object.
    """
    # Find the quantity and time unit
    pattern = r'(\d+)\s+(\w+)'
    match = re.search(pattern, time_str)
    if not match:
        return None

    quantity = int(match.group(1))
    unit = match.group(2).lower()
    now = datetime.now()

    # Determine the time delta based on the unit
    if unit.startswith("min"):
        delta = timedelta(minutes=quantity)
    elif unit.startswith("hour"):
        delta = timedelta(hours=quantity)
    elif unit.startswith("day"):
        delta = timedelta(days=quantity)
    elif unit.startswith("week"):
        delta = timedelta(weeks=quantity)
    elif unit.startswith("month"):
        # Estimate one month as 30 days
        delta = timedelta(days=30 * quantity)
    elif unit.startswith("year"):
        # Estimate one year as 365 days
        delta = timedelta(days=365 * quantity)
    else:
        return None

    return now - delta

def get_jobs(JOB_SETTING):
    id_set = set()
    start = 0
    # 🔹 Retrieve job IDs from LinkedIn
    while len(id_set) < int(JOB_SETTING['max_jobs']):
        list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={JOB_SETTING['title']}&location={JOB_SETTING['location']}&start={start}"
        response = requests.get(list_url)

        # Check if the response is invalid or no more jobs are found
        if response.status_code != 200 or not response.text:
            print("No more jobs found, stopping...")
            break

        list_soup = BeautifulSoup(response.text, "html.parser")
        page_jobs = list_soup.find_all("li")

        if not page_jobs:
            print("No more job postings available.")
            break

        for job in page_jobs:
            base_card_div = job.find("div", {"class": "base-card"})
            if base_card_div:
                job_id = base_card_div.get("data-entity-urn").split(":")[3]
                id_set.add(job_id)  # Using a set ensures duplicate job IDs are automatically filtered out

        print(f"Fetched {len(id_set)} unique job IDs so far...")

        start += 10  # Increase page index by 10 (LinkedIn shows 10 jobs per request)
        time.sleep(2)  # Avoid being blocked by too many requests

    # Convert the set to a list for further processing
    id_list = list(id_set)
    print(f"✅ Total unique jobs collected: {len(id_list)}")
    print(id_list)

    # 🔹 Retrieve detailed information for each job
    job_list = []

    for job_id in id_list:
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        job_response = requests.get(job_url)
        
        if job_response.status_code != 200:
            print(f"⚠️ Failed to fetch job {job_id}")
            continue

        job_soup = BeautifulSoup(job_response.text, "html.parser")
        job_post = {"job_id": job_id}  # Add Job ID to the dictionary
        
        try:
            job_post["job_title"] = job_soup.find("h2", {"class": "top-card-layout__title"}).text.strip()
        except:
            job_post["job_title"] = None

        try:
            job_post["company_name"] = job_soup.find("a", {"class": "topcard__org-name-link"}).text.strip()
        except:
            job_post["company_name"] = None

        try:
            job_post["location"] = job_soup.find("span", {"class": "topcard__flavor topcard__flavor--bullet"}).text.strip()
        except:
            job_post["location"] = None

        try:
            time_text = job_soup.find("span", {"class": "posted-time-ago__text"}).text.strip()
            job_post["time_posted"] = parse_time_posted(time_text)
        except:
            job_post["time_posted"] = None

        try:
            text = job_soup.find("span", {"class": "num-applicants__caption"}).text.strip()
            # Replace " applicants" with an empty string to extract only the number
            num_applicants = text.replace(" applicants", "")
            job_post["num_applicants"] = int(num_applicants)
        except:
            job_post["num_applicants"] = None

        try:
            job_desc_div = job_soup.find("div", class_="show-more-less-html__markup")
            job_post["description"] = job_desc_div.get_text(separator="\n").strip()
        except:
            job_post["description"] = None

        job_list.append(job_post)
        time.sleep(1)  # Avoid being blocked by too many requests
        
    return job_list

# 🔹 Function to insert data into PostgreSQL
def insert_into_postgres(job_list):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    schemaname = 'public'

    insert_query = """
    INSERT INTO {}.linkedin_data_raw (job_id, job_title, company_name, location, time_posted, num_applicants, description)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """.format(schemaname)
    for job in job_list:
        cursor.execute(insert_query, (
            job["job_id"],
            job["job_title"],
            job["company_name"],
            job["location"],
            job["time_posted"],
            job["num_applicants"],
            job["description"]
        ))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    print('Test connection')
    test_conn_resp = test_connection(DB_CONFIG)
    if not test_conn_resp[0]:
        print(test_conn_resp[1])
        exit()
        
    print('Test table')
    test_table_resp = test_table(DB_CONFIG)
    if not test_table_resp:
        exit()
            
    try:
        job_list = get_jobs(JOB_SETTING)
    except Exception as err:
        print(err)
        exit()
    
    try:
        insert_into_postgres(job_list)
    except Exception as err:
        print(err)
        exit()
