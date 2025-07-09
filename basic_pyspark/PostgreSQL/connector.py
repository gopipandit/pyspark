import psycopg2
import os

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="@impandit"
)

# Create a cursor
cur = conn.cursor()

# Run a simple query
cur.execute("SELECT version();")

# Fetch and print result
print(cur.fetchone())

# Close connection
cur.close()
conn.close()