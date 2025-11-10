#!/bin/bash

# ==============================================================================
#                 PostgreSQL Bulk SQL Import Script
# ==============================================================================
#
# Description:
#   This script finds all .sql files in a specified directory and imports
#   them one by one into a target PostgreSQL database using the psql utility.
#
# Usage:
#   1. Configure the variables in the section below.
#   2. Make the script executable: chmod +x import_all.sh
#   3. Run the script: ./import_all.sh
#
# ==============================================================================

set -e

SQL_DIR="/mnt/d/datasets/beijingshi_sql"

DB_HOST="192.168.1.195"
DB_PORT="55432"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWD="ds123456"

export PGPASSWORD=${DB_PASSWD}

if [ ! -d "$SQL_DIR" ]; then
    echo "Error: SQL directory '$SQL_DIR' not found."
    exit 1
fi

shopt -s nullglob
sql_files=("$SQL_DIR"/*.sql)
shopt -u nullglob

if [ ${#sql_files[@]} -eq 0 ]; then
    echo "Warning: No .sql files found in '$SQL_DIR'. Nothing to do."
    exit 0
fi

echo "=================================================="
echo "Starting import into database '$DB_NAME' on host '$DB_HOST'..."
echo "Found ${#sql_files[@]} files to import."
echo "=================================================="

# Loop through all .sql files in the directory
for sql_file in "${sql_files[@]}"; do
    echo -n "Importing file '$(basename "$sql_file")'..."

    # Execute the psql command.
    # The --quiet option reduces verbosity.
    # The --echo-errors option shows errors from within the script.
    # The --on-error-stop option ensures that the script execution stops if an error occurs inside the .sql file.
    psql \
        --host="$DB_HOST" \
        --port="$DB_PORT" \
        --dbname="$DB_NAME" \
        --username="$DB_USER" \
        --quiet \
        --echo-errors \
        --file="$sql_file"

    echo " Done."
done

# Unset the password variable for security
unset PGPASSWORD

echo "=================================================="
echo "All .sql files have been imported successfully!"
echo "=================================================="