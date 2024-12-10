# Use the base image from Apache Airflow
FROM apache/airflow:2.10.2

# Switch to root user to install additional software
USER root

# Update package repositories
RUN apt-get update

# Switch back to the airflow user
USER airflow