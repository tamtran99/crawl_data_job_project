import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

DB_CONFIG = config['Database']
JOB_SETTING = config['Job_Setting']

def init_table(DB_CONFIG) -> str:
    '''
    This function to create initial table in DB
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

    try:    
        cursor.execute(check_query, (
            DB_CONFIG['dbname'],
            table_name
        ))

        conn.commit()
        
        # Fetch the result
        exists = cursor.fetchone()[0]
    
    except Exception as err:
        return err 
    
    
    
    create_table_query = f'''
    CREATE TABLE {table_name} (
        id serial4 NOT NULL,
        job_id int8 NULL,
        job_title text NULL,
        company_name text NULL,
        "location" text NULL,
        time_posted timestamp NULL,
        num_applicants int NULL,
        description text NULL,
        process_date timestamp DEFAULT now() NULL,
        CONSTRAINT linkedin_data_raw_pkey PRIMARY KEY (id)
    );
    '''
    
    try:
        # Execute the query
        
        cursor.execute(create_table_query)

        # Commit the transaction to the database
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
        return "Table created successfully!"
    except Exception as err:
        print(err)
    
init_table(DB_CONFIG)