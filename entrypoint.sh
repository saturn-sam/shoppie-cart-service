#!/bin/sh

set -e
export FLASK_APP=app:app
# Wait for the database to be ready
echo "Waiting for the database..."
DB_HOST=$(echo $DATABASE_URL | sed -n 's/.*@\(.*\):.*/\1/p')
DB_PORT=$(echo $DATABASE_URL | sed -n 's/.*:\([0-9]*\)\/.*/\1/p')
while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.1
done
sleep 5
echo "Database is ready."

# Initialize migrations folder if it doesn't exist
if [ ! -d "migrations" ]; then
  echo "Initializing migrations folder..."
  flask db init
  flask db migrate -m "Initial migration"
fi

# Apply database migrations
echo "Applying database migrations..."
flask db upgrade

# Start the Flask application in production mode
echo "Starting the application..."
exec gunicorn app:app --bind 0.0.0.0:5000 --workers 3 --preload --access-logfile - --error-logfile -