import requests
import os
import sys
import asyncio
import pandas as pd
from datetime import datetime
import logging

import openai as ai
import markdown

import plotly.graph_objects as go
from io import BytesIO

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# Add the parent directory of 'nlsql' to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nlsql.handler import api_post
from nlsql.connectors import connectors


# Email configuration
EMAIL_ADDRESS = os.getenv('EmailAddress', '')
EMAIL_PASSWORD = os.getenv('EmailPassword', '') # User must create app password for gmail/outlook account
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

    # Returned data structure:
    # {
    #     'DataSource-1': [
    #         {
    #             'name': 'table1',
    #             'kpis': ['arg1', 'arg2']
    #         },
    #         {
    #             'name': 'table2',
    #             'kpis': ['arg1', 'arg2', 'arg3']
    #         }
    #     ],
    #     'DataSource-2': [
    #         {
    #             'name': 'table1',
    #             'kpis': ['arg1']
    #         },
    #         {
    #             'name': 'table2',
    #             'kpis': ['arg1', 'arg2']
    #         }
    #     ]
    # }
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

        sensetivity = float(os.getenv('BoundrySensetivity', '2'))

        df['value'] = df['value'].astype(float)

        if df['value'].empty:
            logging.error("All 'value' entries are NaN or non-numeric.")
            return None, None

        # Calculate mean and standard deviation
        mean_value = df['value'].mean()
        std_value = df['value'].std()
        logging.info(f"Mean value: {mean_value}, Standard deviation: {std_value}")
        
        # Define corridors as ±2 standard deviations from the mean
        lower_bound = mean_value - sensetivity * std_value
        upper_bound = mean_value + sensetivity * std_value
        
        return lower_bound, upper_bound
    except Exception as e:
        logging.error(f"Error occurred in calculate_corridors: {e}")
        return None, None


def detect_anomalies(df, lower_bound, upper_bound):
    try:
        anomalies = df[(df['value'] < lower_bound) | (df['value'] > upper_bound)]
        return anomalies
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

  
def reindex_months(df):
    # Ensure DataFrame has all months, 1 to 12
    all_months = pd.DataFrame({'month': range(1, 13)})
    df = pd.merge(all_months, df, on='month', how='left')
    df['value'] = df['value'].astype('float') # Ensure the 'value' column is float
    return df


def generate_graph(trusted_df, comparison_df, kpi, anomalies, lower_bound, upper_bound):

    to_year = int(os.getenv('ToYear'))
    from_year = int(os.getenv('FromYear'))

    # Interpolate missing values in DataFrames
    trusted_df = trusted_df.interpolate(method='linear')
    comparison_df = comparison_df.interpolate(method='linear')

    # Generate a graph reflecting trusted data and anomalous data
    x = trusted_df['month'].tolist()
    x_rev = x[::-1]

    y1 = trusted_df['value'].tolist()
    y2 = comparison_df['value'].tolist()
    anomaly_points = comparison_df[comparison_df['value'].isin(anomalies['value'])]

    upper_bound_list = [upper_bound] * len(x)
    lower_bound_list = [lower_bound] * len(x)
    lower_bound_rev = lower_bound_list[::-1]

    fig = go.Figure()

    # Shaded area for trusted corridors
    fig.add_trace(go.Scatter(
        x=x + x_rev, y=upper_bound_list + lower_bound_rev,
        fill='toself',
        fillcolor='rgba(48, 110, 134, 0.2)',
        line_color='rgba(255,255,255,0)',
        name='Upper & lower bounds',
    ))

    # Trusted line
    fig.add_trace(go.Scatter(
        x=x, y=y1,
        line_color='rgb(0,100,80)',
        name=f'{from_year} - {to_year}',
        mode='lines',
        line={'dash': 'dash'}
    ))

    # Comparison line
    fig.add_trace(go.Scatter(
        x=x, y=y2,
        line_color='rgb(0,176,246)',
        name=f"{kpi}",
    ))

    # Anomaly points
    fig.add_trace(go.Scatter(
        x=anomaly_points['month'], y=anomaly_points['value'],
        mode='markers',
        marker=dict(color='red', size=10),
        name='Anomalies',
    ))

    timeframe = datetime.now().year - (to_year - from_year)

    fig.update_layout(
        title=f"{kpi.title()} from {timeframe} to Present",
        xaxis_title="Month",
        yaxis_title="Value",
        legend_title="Legend",
        template="plotly_white"
    )

    img_buf = BytesIO()
    fig.write_image(img_buf, format='png')
    img_buf.seek(0)
    return img_buf


async def get_response_openai(system_message, messages):
    try:
        ai.api_key = os.getenv('OpenAiAPI')
        ai.api_base = os.getenv('OpenAiBase')
        ai.api_type = os.getenv('OpenAiType')
        ai.api_version = os.getenv('OpenAiVersion')
        deployment_name = os.getenv('OpenAiName')
        
        response = await ai.ChatCompletion.acreate(
            engine=deployment_name,
            messages=create_prompt_openai(system_message, messages),
            temperature=0.5,
            max_tokens=800,
            top_p=0.95,
            frequency_penalty=0,
            presence_penalty=0,
            stop=None
        )
        return response
    except:
        logging.error("There was a problem with OpenAI")
        return "There was a problem generating OpenAI analysis."


def create_prompt_openai(system_content, user_content):
    messages = [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]
    return messages


async def perform_anomaly_check():
    try:
        # Collect necessary data (DataSources, table names, and KPI arguments)
        datasources = retrieve_datasource_names()
        table_data = retrieve_table_data(datasources)

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
                    
                    # Reindex DataFrames to include all months
                    trusted_df = reindex_months(trusted_df)
                    comparison_df = reindex_months(comparison_df)
                    
                    logging.info("---- Trusted DataFrame ----\n")
                    logging.info(trusted_df)
                    logging.info("\n---- Comparison DataFrame ----\n")
                    logging.info(comparison_df)

                    # Calculate the corridors using trusted DataFrame
                    lower_bound, upper_bound = calculate_corridors(trusted_df)
                    logging.info(F"{upper_bound}, {lower_bound}")
                    if lower_bound is None or upper_bound is None:
                        logging.warning(f"Skipping KPI {kpi} due to corridor calculation failure.")
                        continue
                    
                    # Compare comparison DataFrame with calculated corridors and obtain anomalies
                    anomalies = detect_anomalies(comparison_df, lower_bound, upper_bound)
                    
                    logging.info(f"\nAnomalies for KPI {kpi}:\n")
                    logging.info(anomalies)

                    # Form message to send user if anomalies have been detected
                    if not anomalies.empty:
                        # Generate prompt and send to GPT
                        system_message = os.getenv('SystemMessage', 'You are an intelligent data analyzer who will be given trusted data and comparison data. You must give potential reasons to why anomalies are detected in the data based on their KPI names.')
                        
                        user_message = f"KPI: {kpi}, Trusted Data: {trusted_df}, Comparison Data: {comparison_df}, Anomalies Detected: {anomalies}, Sensetivity: {os.getenv('BoundrySensetivity' '2.0')}"

                        response = await get_response_openai(system_message, user_message)
                        response_message = response['choices'][0]['message']['content']

                        # Generate graph image
                        graph = generate_graph(trusted_df, comparison_df, kpi, anomalies, lower_bound, upper_bound)

                        # Append anomaly message and graph together, title message, GPT response, and graph image
                        anomaly_data = (f'There are anomalies with argument: "{kpi}" in table: "{table["name"]}" from datasource: "{data_source}"',
                                        response_message,
                                        graph)
                        anomaly_messages.append(anomaly_data)

        if anomaly_messages:
            # Send email to user
            if EMAIL_ADDRESS and EMAIL_PASSWORD and RECIPIENT_EMAIL:
                send_email(anomaly_messages)
            else:
                logging.warning("Email credentials missing")
        await conn.close()

    except Exception as e:
        logging.error(f"Failed to perform anomaly check: {e}")
        await conn.close()


def send_email(anomaly_messages):
    try:
        msg = MIMEMultipart()
        msg['Subject'] = "Anomaly Detection Report"
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = RECIPIENT_EMAIL

        n = 1
        html_content = "<h3>Anomaly Detection Report</h3>"
        for message in anomaly_messages:
            html_content += "<p>"
            html_content += f"{message[0]}<br>"
            html_content += f"{markdown.markdown(message[1])}<br>"
            html_content += "</p>"

            img_data = message[2].getvalue()
            img = MIMEImage(img_data, 'png')
            img.add_header('Content-ID', f'<anomaly_graph_{n}>')  # Content-ID for inline images
            img.add_header('Content-Disposition', 'inline', filename=f'anomaly_graph_{n}.png')
            msg.attach(img)

            # Add image tag in HTML content
            html_content += f"<img src='cid:anomaly_graph_{n}'>"
            n += 1


        # Add HTML content to email
        msg.attach(MIMEText(html_content, 'html'))

        # Send email using SMTP
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


async def main():
    # Script will run in background with asyncio and repeat every 1 hour (3600 seconds)
    while True:
        try:
            await perform_anomaly_check()
            await asyncio.sleep(3600 * 4) # Repeat every hour
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            await asyncio.sleep(3600 * 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
