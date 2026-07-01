#!/bin/bash

# Initialization script for the official postgres image, per
#   https://hub.docker.com/_/postgres/
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "ICAT";
    CREATE USER irods WITH PASSWORD 'testpassword';
    GRANT ALL PRIVILEGES ON DATABASE "ICAT" to irods;
    ALTER DATABASE "ICAT" OWNER TO irods;
EOSQL
