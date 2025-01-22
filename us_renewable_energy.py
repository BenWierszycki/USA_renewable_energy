# import initial packages
import requests
import pandas as pd

# Fetch energy data for all US states
def get_energy_data_all_states():

    # API key for EIA data
    api_key = "Xu8bhXpAJXIHUGTtps3GgKutiKK31fuYXmQQeaPK"
    
    # Base URL for EIA API
    api_url = "https://api.eia.gov/v2/seds/data/"
    
    state_codes = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
        "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
        "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
        "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
        "WI", "WY"
    ]

    all_data = []

    # Loop through each state to fetch data
    for state in state_codes:

        #  API request parameters
        params = {
            "frequency": "annual",
            "data[0]": "value",
            "facets[stateId][]": state,
            "facets[seriesId][]": ["TETCB", "RETCB", "FFTCB", "REPRB", "TEPRB"],
            "start": "2008",
            "end": "2100",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": "0",
            "length": "500",
            "api_key": api_key
        }
        
        response = requests.get(api_url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse JSON response if it contains data
            data = response.json()
            if 'response' in data and 'data' in data['response']:
                df = pd.DataFrame(data['response']['data'])  # Convert data to DataFrame
                all_data.append(df)  # Add data for this state to the list

    # Combine data into one DF
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Ensure 'value' column is numeric for calculations
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        
        # Reshape the DataFrame to have one row per state and year
        pivoted_df = combined_df.pivot_table(
            index=['stateDescription', 'stateId', 'period'], 
            columns='seriesDescription', 
            values='value',
            aggfunc='first'
        ).reset_index()

        # Rename columns
        pivoted_df.rename(columns={
            'period': '_year_', 
            'stateId': 'state_code', 
            'stateDescription': 'state', 
            'Fossil fuels total consumption': 'fossil_fuels_total_consumption',
            'Renewable energy production': 'renewable_energy_production',
            'Renewable energy total consumption': 'renewable_energy_total_consumption',
            'Total energy consumption': 'total_energy_consumption',
            'Total primary energy production': 'total_primary_energy_production'
        }, inplace=True)

        return pivoted_df

# Call the function
pivoted_energy_df_all_states = get_energy_data_all_states()


# ----------------------------------------------------------------------------------


# Database connection setup
import psycopg2 as psql
from dotenv import load_dotenv
load_dotenv()
import os

user = os.getenv('user')
password = os.getenv('password')
my_host = os.getenv('host')

# Connect to the PostgreSQL database
conn = psql.connect(
    database="pagila",
    user=user,
    host=my_host,
    password=password,
    port=5432
)

cur = conn.cursor()

# Create table
cur.execute("""
CREATE TABLE IF NOT EXISTS student.bw_us_energy (
    state VARCHAR,
    state_code VARCHAR,
    _year_ VARCHAR,
    fossil_fuels_total_consumption INT,
    renewable_energy_production INT,
    renewable_energy_total_consumption INT,
    total_energy_consumption INT,
    total_primary_energy_production INT
)
""")
conn.commit() 

# Add data to table
insert_data = """
INSERT INTO student.bw_us_energy (
    state,
    state_code,
    _year_,
    fossil_fuels_total_consumption,
    renewable_energy_production,
    renewable_energy_total_consumption,
    total_energy_consumption,
    total_primary_energy_production
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Loop through the DataFrame and insert each row
for index, row in pivoted_energy_df_all_states.iterrows():
    values = (
        row['state'],
        row['state_code'],
        row['_year_'],
        row.get('fossil_fuels_total_consumption'),
        row.get('renewable_energy_production'),
        row.get('renewable_energy_total_consumption'),
        row.get('total_energy_consumption'),
        row.get('total_primary_energy_production')
    )

    cur.execute(insert_data, values) 

conn.commit()


# ----------------------------------------------------------------------------------
## ADDING STATE CONTROL DATA TO DATABASE


# Load a CSV file with state control data
csv_file_path = r'\Users\benjw\state_control_new.csv'
df = pd.read_csv(csv_file_path)  

cur = conn.cursor()

# Create table
create_table_query = """
CREATE TABLE IF NOT EXISTS student.bw_us_state_control (
    {}
);
"""

# Create table columns based on CSV file headers
column_definitions = ", ".join(f"{col} VARCHAR" for col in df.columns)

cur.execute(create_table_query.format(column_definitions))
conn.commit()

# Insert data into table
insert_query = f"""
INSERT INTO student.bw_us_state_control ({', '.join(df.columns)}) 
VALUES ({', '.join(['%s' for _ in df.columns])})
"""

# Convert DataFrame rows to a list of tuples for insertion
data = [tuple(row) for row in df.itertuples(index=False, name=None)]

cur.executemany(insert_query, data)
conn.commit()

print("Data loaded successfully into student.bw_us_state_control.")


# Create a new table by joining the energy data and state control data
join_tables_query = """
CREATE TABLE IF NOT EXISTS student.bw_us_energy_state_control AS
SELECT e.*, sc.state_control 
FROM student.bw_us_energy e 
LEFT JOIN student.bw_us_state_control sc
ON e.state = sc.state AND e._year_ = sc._year_;
"""

cur.execute(join_tables_query)
conn.commit()  
conn.close() 


# ----------------------------------------------------------------------------------
## EXPORTING DATA FOR TABLEAU VISUALISATIONS


import psycopg2 as psql
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

# Table to export and path
table_name = 'student.bw_us_energy_state_control'
csv_file_path = r'C:\Users\benjw\OneDrive\Documents\Digital Futures\Post academy\us_energy_state_control.csv'

user = os.getenv('user')
password = os.getenv('password')
my_host = os.getenv('host')

conn = psql.connect(
    database="pagila",
    user=user,
    host=my_host,
    password=password,
    port=5432
)

# Use pandas to query the table and read data into DF
query = f'SELECT * FROM {table_name}'
df = pd.read_sql(query, conn)

# Export the DF to a CSV file
df.to_csv(csv_file_path, index=False)

conn.close()



