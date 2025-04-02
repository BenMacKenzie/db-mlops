export DATABRICKS_WAREHOUSE_ID=4b9b953939869799
export DATABRICKS_HOST=https://e2-demo-field-eng.cloud.databricks.com/
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/4b9b953939869799"


brew install postgresql@14
brew services start postgresql@14
createdb demo
psql demo -c "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP); INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com'), ('Jane Smith', 'jane@example.com'), ('Bob Johnson', 'bob@example.com');"

psql demo -c "SELECT * FROM users;"
