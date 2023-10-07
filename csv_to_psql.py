import psycopg2
import pandas as pd
from decouple import config 

# Read configuration variables from the .env file
DATABASE_HOST = config('DATABASE_HOST')
DATABASE_NAME = config('DATABASE_NAME')
DATABASE_USER = config('DATABASE_USER')
DATABASE_PASSWORD = config('DATABASE_PASSWORD')

# Database connection parameters
db_params = {
    'host': DATABASE_HOST,
    'database': DATABASE_NAME,
    'user': DATABASE_USER,
    'password': DATABASE_PASSWORD,
}

csv_file_path = 'housing.csv'

table_name = DATABASE_NAME

try:
    #connect to the Postgres database
    conn = psycopg2.connect(**db_params)

    #create a cursor object
    cursor = conn.cursor()

    #Read csv file to infer column names and data types
    df = pd.read_csv(csv_file_path, nrows = 1)
    columns =df.columns.tolist()
    column_datata_types = [f"{column} TEXT" for column in columns]

    #create table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(f"{column} TEXT" for column in columns)}
    );
    """
 # Execute the table creation  statement
    cursor.execute(create_table_sql)

    # Commit the transaction
    conn.commit()

    #copy csv table contents into postgres
    copy_sql = f""" 
        COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER as ','
        """

    #check for existing data before inserting
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    data_exists = cursor.fetchone()[0]


    #open and copy the CSV file into the database
    if data_exists == 0:
        with open(csv_file_path,'r') as f:
            cursor.copy_expert(sql=copy_sql, file=f)

        #commit the transaction
        conn.commit()
    else:
        print("Data already exist in the table. Skipping insertion")

except (Exception, psycopg2.Error) as e:
    print("Error:", e)

#close cursor and connection
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()