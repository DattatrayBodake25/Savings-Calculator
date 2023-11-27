#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import packages
import psycopg2
from datetime import datetime, timedelta
import json


# In[2]:


# Helper function to calculate age based on birthdate and reference date
def calculate_age(birthdate, reference_date):
    # Check if the birthdate is already a datetime object
    if isinstance(birthdate, datetime):
        # If it is, convert it to a string in the format "%Y-%m-%d"
        birthdate = birthdate.strftime("%Y-%m-%d")

    # Convert birthdate to a datetime object
    birthdate = datetime.strptime(birthdate, "%Y-%m-%d")

    # Calculate age in years
    age = (reference_date - birthdate).days // 365

    # Round the age to the nearest integer
    return round(age)


# Main function to calculate savings for each age group
def calculate_savings(events, context):
    try:
        # Extracting connection details from the events dictionary
        db_name = events["database"]
        db_port = events["port"]
        db_host = events["host"]
        db_username = events["username"]
        db_password = events["password"]
        reference_date_str = events["date"]

        # Establishing connection to the PostgreSQL database
        connection = psycopg2.connect(dbname=db_name,
                                      user=db_username,
                                      password=db_password,
                                      host=db_host,
                                      port=db_port)

        # Creating a cursor to interact with the database
        cursor = connection.cursor()

        # Reading transactions from the specified date
        reference_date = datetime.strptime(reference_date_str, "%d/%m/%Y")
        cursor.execute(
            """
            SELECT c.customer_id, c.date_of_birth, t.txn_type, t.txn_amount
            FROM customer c
            JOIN transactions t ON c.customer_id = t.customer_id
            WHERE t.transaction_date::date = %s
            """, (reference_date, ))

        # Dictionary to store savings for each age group
        age_savings = {}

        # Loop through the retrieved database rows
        for row in cursor.fetchall():
            customer_id, birthdate, txn_type, txn_amount = row

            # Calculate age based on birthdate and reference date
            age = calculate_age(birthdate, reference_date)

            # Determine savings based on transaction type
            savings = txn_amount if txn_type == 'CREDIT' else -txn_amount

            # Update age_savings dictionary
            if age in age_savings:
                age_savings[age].append(savings)
            else:
                age_savings[age] = [savings]

        # Closing the cursor and connection
        cursor.close()
        connection.close()

        # Calculating average savings for each age group
        result_data = {}
        for age, savings_list in age_savings.items():
            avg_saving = round(sum(savings_list) / len(savings_list))
            result_data[age] = avg_saving

        # Returning the result as a JSON payload
        result_payload = {"statusCode": 200, "data": result_data}
        return json.dumps(result_payload)

    except Exception as e:
        # Handling exceptions and returning an error payload
        error_payload = {"statusCode": 400, "message": str(e)}
        return json.dumps(error_payload)


# Example usage:
# Define the events dictionary with database connection details and the date
events = {
    "database": "moneyclub",
    "port": 5432,
    "host": "localhost",
    "username": "postgres",
    "password": "Datta@2505",
    "date": "01/01/2023"
}

# Call the calculate_savings function with the events dictionary
result = calculate_savings(events, None)

# Print the result
print(result)


# In[3]:


#output
#{"statusCode": 200, "data": {"32": 150, "37": 1000, "27": 300, "42": 25}}


# In[4]:


#Thank you!

