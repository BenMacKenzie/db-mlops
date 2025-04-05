import os
import psycopg2
from config_manager import ConfigManager

def create_tables():
    """Create all necessary database tables."""
    # Get database configuration
    config = ConfigManager()
    db_config = config.get_database_config()
    
    # Connect to database
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']
    )
    
    try:
        # Read and execute SQL files
        sql_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql')
        sql_file = os.path.join(sql_dir, 'tables.sql')
        
        with conn.cursor() as cur:
            # Read and execute the SQL file
            with open(sql_file, 'r') as f:
                sql = f.read()
                cur.execute(sql)
            
            conn.commit()
            print("Successfully created all tables.")
            
    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    create_tables() 