import psycopg2

def create_schema():
    conn = psycopg2.connect(
        host="localhost",
        port="5433",
        database="tech_trends",
        user="admin",
        password="password123"
    )
    cursor = conn.cursor()
    
    # Create tech_skill_counts table with UNIQUE constraint on skill for UPSERTs
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tech_skill_counts (
        skill VARCHAR(100) PRIMARY KEY,
        mention_count INT NOT NULL DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ PostgreSQL table 'tech_skill_counts' initialized successfully!")

if __name__ == "__main__":
    create_schema()