# Install Airflow
pip install apache-airflow

# Initialize the Airflow database (creates the backend SQLite db for metadata)
airflow db init

# Create an admin user so you can log into the UI
airflow users create \
    --username admin \
    --firstname Data \
    --lastname Engineer \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start the web server and scheduler (run these in separate terminal windows)
airflow webserver --port 8080
airflow scheduler