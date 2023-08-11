#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  2 13:21:14 2023

@author: Yekta
"""


import csv
import sqlite3

def create_table_if_not_exists(connection):
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_table (
            title Text,
            url TEXT PRIMARY KEY,
            category TEXT) ''')
    connection.commit()

def insert_data_from_csv(connection, csv_file):
    cursor = connection.cursor()
    with open(csv_file, 'r' , encoding='utf-8', errors='ignore') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            cursor.execute('INSERT OR IGNORE INTO data_table (title, url, category) VALUES (?, ?,?)', row)
            # Replace column1, column2, column3 with the actual column names in the table
            connection.commit()

if __name__ == "__main__":

        csv_file_path = "links-of-b.csv"
        db_file_path = "news.db"
    
        # Establish a connection to the SQLite database
        connection = sqlite3.connect(db_file_path)
    
        # Create the table if it doesn't exist
        create_table_if_not_exists(connection)
    
        # Insert data from CSV into the database
        insert_data_from_csv(connection, csv_file_path)
    
        # Close the connection
        connection.close()
