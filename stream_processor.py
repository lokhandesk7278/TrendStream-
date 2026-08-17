import json
import psycopg2
from kafka import KafkaConsumer

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port="5433",
        database="tech_trends",
        user="admin",
        password="password123"
    )

consumer = KafkaConsumer(
    'tech-jobs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='trendstream-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("⚡ [TrendStream] Processor active: Ingesting from Kafka & writing to PostgreSQL...\n")

conn = get_db_connection()
cursor = conn.cursor()

upsert_query = """
INSERT INTO tech_skill_counts (skill, mention_count, last_updated)
VALUES (%s, 1, CURRENT_TIMESTAMP)
ON CONFLICT (skill) 
DO UPDATE SET 
    mention_count = tech_skill_counts.mention_count + 1,
    last_updated = CURRENT_TIMESTAMP;
"""

try:
    for message in consumer:
        event = message.value
        skills = event.get("required_skills", [])
        
        for skill in skills:
            cursor.execute(upsert_query, (skill,))
        
        conn.commit()
        print(f"🔄 Processed event: {event['job_title']} | Updated counts for: {skills}")

except KeyboardInterrupt:
    print("\n🛑 Stream processor stopped.")
finally:
    cursor.close()
    conn.close()