#!/bin/bash
set -e

# Initialize the database
airflow db init

# Create admin user if it doesn't exist
if [ ! -f "/opt/airflow/.user_created" ]; then
  echo "Creating admin user"
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin
  touch /opt/airflow/.user_created
  echo "User \"admin\" created with role \"Admin\""
fi

# Execute the command passed to docker run
if [[ "$1" == "standalone" ]]; then
  exec airflow standalone
else
  exec "$@"
fi