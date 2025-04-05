-- Projects table
CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    target VARCHAR(255) NOT NULL,
    job_id VARCHAR(255)
); 