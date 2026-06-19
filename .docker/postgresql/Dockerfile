FROM bitnami/postgresql:latest
USER postgres
COPY ./sql/add_api_url_column.sql /docker-entrypoint-initdb.d/
COPY ./sql/add_pipeline_mode_columns.sql /docker-entrypoint-initdb.d/
COPY ./sql/create_airflow_database.sql /docker-entrypoint-initdb.d/
COPY ./sql/create_cleaning_state_table.sql /docker-entrypoint-initdb.d/
# create_debezium_user.sh is not copied here: its bash heredoc breaks on
# Windows checkouts (CRLF line endings). Run it manually instead - see
# README.md "Debezium user creation".
# COPY ./sql/create_debezium_user.sh /docker-entrypoint-initdb.d/
COPY ./sql/schema.sql /docker-entrypoint-initdb.d/