#!/bin/bash
mkdir -p .streamlit

# Create the secrets.toml file using your Railway variables
echo "[postgres]" > .streamlit/secrets.toml
echo "host = \"$DB_HOST\"" >> .streamlit/secrets.toml
echo "port = $DB_PORT" >> .streamlit/secrets.toml
echo "database = \"$dbname\"" >> .streamlit/secrets.toml
echo "user = \"$user\"" >> .streamlit/secrets.toml
echo "password = \"$password\"" >> .streamlit/secrets.toml

# Replace 'your_app_file.py' with the actual name of your main python file
streamlit run dashboard.py --server.port $PORT --server.address 0.0.0.0
