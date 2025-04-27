#!/bin/bash

# Remote database details
REMOTE_HOST="3.76.40.121"
REMOTE_PORT="5432"
REMOTE_USER="postgres"
REMOTE_PASSWORD="336699"
REMOTE_DB="trip_dw"

# Local database details
LOCAL_DB="trip_dw_local"
LOCAL_USER="postgres"
LOCAL_PASSWORD="336699"
LOCAL_HOST="localhost"
LOCAL_PORT="5432"

# Export password for PostgreSQL commands
export PGPASSWORD="${REMOTE_PASSWORD}"

echo "Creating exact duplicate of ${REMOTE_DB} from remote server to local database..."

# First, dump the remote database to a file
echo "Dumping ${REMOTE_DB} from remote server to a temporary file..."
pg_dump -h ${REMOTE_HOST} -p ${REMOTE_PORT} -U ${REMOTE_USER} -Fc ${REMOTE_DB} > remote_db_backup.dump

# Change password for local operations
export PGPASSWORD="${LOCAL_PASSWORD}"

# Check if local database exists and drop it if it does
echo "Checking if local database exists..."
if psql -h ${LOCAL_HOST} -p ${LOCAL_PORT} -U ${LOCAL_USER} -lqt | cut -d \| -f 1 | grep -qw ${LOCAL_DB}; then
    echo "Local database ${LOCAL_DB} exists, dropping it..."
    dropdb -h ${LOCAL_HOST} -p ${LOCAL_PORT} -U ${LOCAL_USER} ${LOCAL_DB}
fi

# Create local database
echo "Creating local database ${LOCAL_DB}..."
createdb -h ${LOCAL_HOST} -p ${LOCAL_PORT} -U ${LOCAL_USER} ${LOCAL_DB}

# Restore from the dump to the local database
echo "Restoring dump to local database ${LOCAL_DB}..."
pg_restore -h ${LOCAL_HOST} -p ${LOCAL_PORT} -U ${LOCAL_USER} -d ${LOCAL_DB} remote_db_backup.dump

# Verify the restore was successful
echo "Verifying the restore..."
echo "Tables in restored database:"
psql -h ${LOCAL_HOST} -p ${LOCAL_PORT} -U ${LOCAL_USER} -d ${LOCAL_DB} -c "\dt dw.*"

echo "Row counts in some key tables:"
psql -h ${LOCAL_HOST} -p ${LOCAL_PORT} -U ${LOCAL_USER} -d ${LOCAL_DB} -c "SELECT 'dw.dim_region' as table_name, COUNT(*) FROM dw.dim_region UNION ALL SELECT 'dw.dim_region_mapping' as table_name, COUNT(*) FROM dw.dim_region_mapping;"

# Cleanup
echo "Cleaning up..."
rm remote_db_backup.dump

echo "Database clone completed. The database ${LOCAL_DB} has been created with data from the remote ${REMOTE_DB}."
echo "Run 'docker-compose up' to start the application with the local database."

# Unset password
unset PGPASSWORD 