import requests
import os
import sys
import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
import logging

import smtplib
from email.message import EmailMessage

# Add the parent directory of 'nlsql' to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import `api_post` from `handler.py`
from nlsql.handler import api_post
from nlsql.connectors import connectors

# use info for test purposes. Script should be integrated with connectors to get proper user/db info.
# info = {
#     'DataSource': os.getenv('DataSource'),
#     'DbUser': os.getenv('DbUser'),
#     'DbPassword': os.getenv('DbPassword'),
#     'DbName': os.getenv('DbName'),
#     'DbPort': int(os.getenv('DbPort', '5432'))
# }

# Email configuration
# Environmental variables should be used here outside of testing
EMAIL_ADDRESS = os.getenv('EmailAddress', '')
EMAIL_PASSWORD = os.getenv('EmailPassword', '') # User must create app password for gmail account
RECIPIENT_EMAIL = os.getenv('RecipientEmail', '')

def retrieve_datasource_names():
    try:
        headers = {
            "Authorization": 'Token ' + os.getenv("ApiToken")
        }
    
        # Request DataSource names
        url = "https://api.nlsql.com/v1/data-source/"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data_sources = response.json()

        # Append DataSource names to a list
        datasource_names = []
        for data_source in data_sources:
            datasource_names.append(data_source['name'])
        
        return datasource_names
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve datasource names: {e}")
        return []


def retrieve_table_data(datasource_names):
    headers = {
        "Authorization": 'Token ' + os.getenv("ApiToken", '')
    }

    base_url = "https://api.nlsql.com/v1/data-source/"
    table_data = {}

    try:
        # Loop DataSource names and request 
        for name in datasource_names:
            response = requests.get(base_url + name, headers=headers)
            response.raise_for_status()
            data = response.json()['tables']
            table_data[name] = []
            for table in data:
                table_data[name].append({
                    'name': table['table_name'], 
                    'kpis': [arg['argument'] for column in table['columns'] for arg in column['column_other_params']['arguments'] if column['column_other_params']['arguments']]
                })
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve table data for {name}: {e}")

    return table_data


async def send_nl_prompt(kpi_arg):
    try:
        # Perform kpi request using user-specified timeframes
        trusted_from_year = int(os.getenv('FromYear'))
        to_year = int(os.getenv('ToYear'))
        
        # form NL strings and request data
        message = f'{kpi_arg} between {trusted_from_year} and {to_year} by month'
        trusted_values = await api_post(message)

        # Check that the KPI arg has a relevant calculation (sum or average)
        if 'SUM' in trusted_values['sql'] or 'AVG' in trusted_values['sql']:
            timeframe = to_year - trusted_from_year
            current_year = datetime.now().year
            from_year = current_year - timeframe

            message = f'{kpi_arg} between {from_year} and {current_year} by month'
            comparison_values = await api_post(message)

            # Return the trusted values and values to compare against
            return trusted_values, comparison_values

        return None, None
    except Exception as e:
        logging.error(f"Failed to send NL prompt for {kpi_arg}: {e}")
        return None, None


def calculate_corridors(df):
    try:
        # Calculate the corridors of a trusted dataset
        df['value'] = df['value'].astype(float)  # Ensure all values are float
        corridors = df['value'].quantile([0.25, 0.75])
        return corridors
    except Exception as e:
        logging.error(f"Failed to calculate corridors: {e}")
        return None


def detect_anomalies(df, corridors):
    try:
        # Compare calculated corridors with comparison dataset
        df['value'] = df['value'].astype(float)
        anomalies = []
        lower_bound = corridors.loc[0.25]
        upper_bound = corridors.loc[0.75]
        # Loop values, if value is outside calculated quartile boundaries, append detected anomalies to list
        for value in df['value']:
            if value < lower_bound or value > upper_bound:
                anomalies.append(value)
        return anomalies
    except Exception as e:
        logging.error(f"Failed to detect anomalies: {e}")
        return []


async def perform_anomaly_check():
    try:
        # Collect necessary data (DataSources, table names, and KPI arguments)
        datasources = retrieve_datasource_names()
        table_data = retrieve_table_data(datasources)

        # Connect to the user's database (for testing)
        # conn = await asyncpg.connect(
        #     host=info['DataSource'],
        #     user=info['DbUser'],
        #     password=info['DbPassword'],
        #     database=info['DbName'],
        #     port=int(info['DbPort']),
        #     timeout=120,
        # )

        # Get user's db type
        db = os.getenv('DatabaseType', 'postgresql').lower()

        # Prepare connection parameters
        db_params = await connectors.get_db_param(db)

        # Connect to the user's database using get_connector
        conn = await connectors.get_connector(db, **db_params)

        anomaly_messages = []
        # Loop through the KPI arguments
        for data_source, tables in table_data.items():
            for table in tables:
                for kpi in table['kpis']:
                    # Collect the SQL queries for trusted and comparison data
                    trusted_sql, comparison_sql = await send_nl_prompt(kpi)
                    if trusted_sql is None or comparison_sql is None:
                        logging.info(f"Skipping KPI {kpi} due to lack of SUM or AVG in SQL.")
                        continue
                    logging.info(f"SQL: {trusted_sql['sql']}")
                    logging.info(f"SQL: {comparison_sql['sql']}")
                    # Query the user's database to retrieve the trusted and comparison data
                    trusted_result = await connectors.do_query(db, conn, trusted_sql['sql'])
                    comparison_result = await connectors.do_query(db, conn, comparison_sql['sql'])
                    logging.info(f"Trusted result: {trusted_result}")
                    logging.info(f"Comparison result: {comparison_result}")

                    # Convert data to Pandas DataFrames
                    trusted_df = pd.DataFrame(trusted_result, columns=['value', 'month'])
                    comparison_df = pd.DataFrame(comparison_result, columns=['value', 'month'])
                    
                    logging.info("---- Trusted DataFrame ----\n")
                    logging.info(trusted_df)
                    logging.info("\n---- Comparison DataFrame ----\n")
                    logging.info(comparison_df)

                    # Calculate the corridors using trusted DataFrame
                    corridors = calculate_corridors(trusted_df)
                    if corridors is None:
                        logging.warning(f"Skipping KPI {kpi} due to corridor calculation failure.")
                        continue
                    
                    # Compare comparison DataFrame with calculated corridors and obtain anomalies
                    anomalies = detect_anomalies(comparison_df, corridors)
                    
                    logging.info(f"\nAnomalies for KPI {kpi}:\n")
                    logging.info(anomalies)

                    # Form message to send user if anomalies have been detected
                    if anomalies:
                        anomaly_message = f'There are anomalies with argument: {kpi} in table: {table["name"]} from datasource: {data_source}\n\n'
                        anomaly_message += "---- Trusted DataFrame ----\n"
                        anomaly_message += f"{trusted_df}\n\n"
                        anomaly_message += "---- Comparison DataFrame ----\n"
                        anomaly_message += f"{comparison_df}\n\n"
                        anomaly_message += f"Anomalies for KPI {kpi}:\n"
                        anomaly_message += f"{anomalies}"
                        anomaly_message += "\n\n------------------------------\n\n"
                        anomaly_messages.append(anomaly_message)

        if anomaly_messages:
            if EMAIL_ADDRESS != '' and EMAIL_PASSWORD != '' and RECIPIENT_EMAIL != '':
                try:
                    send_email(anomaly_messages)
                except Exception as e:
                    logging.error(f"Failed to send email: {e}")
            else:
                logging.warning("Email credentials missing")


        await conn.close()
    except Exception as e:
        logging.error(f"Failed to perform anomaly check: {e}")
        await conn.close()


def send_email(messages):
    try:
        # Create EmailMessage object
        msg = EmailMessage()
        msg.set_content("\n\n".join(messages))

        msg['Subject'] = 'Anomaly Detected in NLSQL Data'
        msg['From'] = 'NLSQL Bot'
        msg['To'] = RECIPIENT_EMAIL

        # Send email using SMTP
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


async def main():
    # Script will run in background with asyncio and repeat every 1 hour
    while True:
        try:
            await perform_anomaly_check()
            await asyncio.sleep(3600) # Repeat every hour
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            await asyncio.sleep(3600)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
