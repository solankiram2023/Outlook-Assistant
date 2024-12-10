# Use the base image from Apache Airflow
FROM apache/airflow:2.10.2

# Switch to the airflow user (default in the base image)
USER airflow

# Copy requirements.txt into the container
COPY requirements.txt /requirements.txt

# # Copy the .env file into the container
COPY .env /opt/airflow/dags/.env

# Install Python dependencies as the airflow user
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to the airflow user explicitly
USER airflow