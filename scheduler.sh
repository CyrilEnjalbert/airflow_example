# Set the Airflow home directory (if not already set)
export AIRFLOW_HOME=~/airflow

## Initialize the database
#airflow db init
#
## Create an admin user for the web interface (with consistent credentials)
## Delete existing admin if it exists
#airflow users delete -u admin
#
## Create a new admin user with simple password
#airflow users create \
#    --username admin \
#    --firstname Admin \
#    --lastname User \
#    --role Admin \
#    --email admin@example.com \
#    --password admin

# Kill any existing Airflow processes
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true
pkill -f "airflow api-server" || true
pkill -f "airflow standalone" || true

# Remove any PID files to prevent "already running" errors
rm -f ~/airflow/scheduler.pid || true
rm -f ~/airflow/webserver.pid || true
rm -f ~/airflow/api-server.pid || true

## Start the API server (replaces the deprecated webserver command)
#airflow api-server -p 8114 --daemon
#
### Start the scheduler
##airflow scheduler --daemon
#
#echo "Airflow is running!"
#echo "Access the UI at: http://localhost:8114"
#echo "Username: admin"
#echo "Password: admin"
#
## Start the scheduler
#airflow scheduler --daemon