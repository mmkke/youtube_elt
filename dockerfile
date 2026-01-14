ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

# Copy requirements into the image
COPY requirements.txt /

# Install Python deps as root, then drop back to airflow user
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt