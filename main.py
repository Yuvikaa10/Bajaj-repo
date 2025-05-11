import sqlite3
import pandas as pd
import requests
import os
import json

def generate_webhook(url, payload):
    
    response = requests.post(url, json=payload)
    response.raise_for_status()

    data = response.json()
   
    webhook = data['webhook']
    
    accessToken = data['accessToken']
   
    return webhook , accessToken

def create_connection(db_name="acropolis.db"):

    if os.path.exists(db_name):
        os.remove(db_name)
    return sqlite3.connect(db_name)

def create_tables(cursor):
    

    cursor.execute("DROP TABLE IF EXISTS DEPARTMENT;")
    cursor.execute("DROP TABLE IF EXISTS EMPLOYEE;")

    cursor.execute("""
    CREATE TABLE DEPARTMENT (
        DEPARTMENT_ID INTEGER PRIMARY KEY,
        DEPARTMENT_NAME TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE EMPLOYEE (
        EMP_ID INTEGER PRIMARY KEY,
        FIRST_NAME TEXT,
        LAST_NAME TEXT,
        DOB DATE,
        GENDER TEXT,
        DEPARTMENT INTEGER,
        FOREIGN KEY (DEPARTMENT) REFERENCES DEPARTMENT(DEPARTMENT_ID)
    );
    """)

def insert_data(cursor):

    departments = [
        (1, 'HR'), (2, 'Finance'), (3, 'Engineering'),
        (4, 'Sales'), (5, 'Marketing'), (6, 'IT')
    ]
    employees = [
        (1, 'John', 'Williams', '1980-05-15', 'Male', 3),
        (2, 'Sarah', 'Johnson', '1990-07-20', 'Female', 2),
        (3, 'Michael', 'Smith', '1985-02-10', 'Male', 3),
        (4, 'Emily', 'Brown', '1992-11-30', 'Female', 4),
        (5, 'David', 'Jones', '1988-09-05', 'Male', 5),
        (6, 'Olivia', 'Davis', '1995-04-12', 'Female', 1),
        (7, 'James', 'Wilson', '1983-03-25', 'Male', 6),
        (8, 'Sophia', 'Anderson', '1991-08-17', 'Female', 4),
        (9, 'Liam', 'Miller', '1979-12-01', 'Male', 1),
        (10, 'Emma', 'Taylor', '1993-06-28', 'Female', 5),
    ]
    cursor.executemany("INSERT INTO DEPARTMENT VALUES (?, ?);", departments)
    cursor.executemany("INSERT INTO EMPLOYEE VALUES (?, ?, ?, ?, ?, ?);", employees)

def query_younger_employees_by_department(conn, query):
    return pd.read_sql_query(query, conn)

def post_sql_query_to_webhook(url , access_token, payload):
    
    headers = {
        "Authorization": access_token,
        "Content-Type": "application/json"
    }
    

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return {
        "status": "success",
        "data": response.json()
    }

def main():
   
    first_url = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/PYTHON"
    payload = {
                "name": "Yuvika Bansal" ,
                "regNo": "0827CY221074" ,
                "email": "yuvikabansal220888@acropolis.in"
                }
    webhook , accessToken = generate_webhook(first_url, payload)
   

    conn = create_connection()
    cursor = conn.cursor()

    create_tables(cursor)
    insert_data(cursor)
    conn.commit()
    
    query = """
    SELECT 
        e1.EMP_ID,
        e1.FIRST_NAME,
        e1.LAST_NAME,
        d.DEPARTMENT_NAME,
        COUNT(e2.EMP_ID) AS YOUNGER_EMPLOYEES_COUNT
    FROM EMPLOYEE e1
    JOIN DEPARTMENT d ON e1.DEPARTMENT = d.DEPARTMENT_ID
    LEFT JOIN EMPLOYEE e2 
        ON e1.DEPARTMENT = e2.DEPARTMENT 
        AND DATE(e2.DOB) > DATE(e1.DOB)
    GROUP BY 
        e1.EMP_ID, e1.FIRST_NAME, e1.LAST_NAME, d.DEPARTMENT_NAME
    ORDER BY e1.EMP_ID DESC;
    """
    result_df = query_younger_employees_by_department(conn, query)
    
    print("Younger Employees by Department:\n", result_df)
    
    second_url = "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/PYTHON"
    
    payload = {
        "finalQuery": query
    }
    
    response = post_sql_query_to_webhook(second_url, accessToken,  payload)
    print(response)
    conn.close()


if __name__ == "__main__":
    main()