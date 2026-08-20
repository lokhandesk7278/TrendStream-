import json
import psycopg2
from confluent_kafka import Consumer, KafkaError

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port="5433",
        database="tech_trends",
        user="admin",
        password="password123"
    )

# Configure Confluent Kafka Consumer (bypasses Windows socket bugs)
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'trendstream-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe(['tech-jobs'])

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
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"⚠️ Consumer error: {msg.error()}")
                break

        # Decode JSON payload
        event = json.loads(msg.value().decode('utf-8'))
        skills = event.get("required_skills", [])
        
        for skill in skills:
            cursor.execute(upsert_query, (skill,))
        
        conn.commit()
        print(f"🔄 Processed event: {event.get('job_title', 'Unknown')} | Updated counts for: {skills}")

except KeyboardInterrupt:
    print("\n🛑 Stream processor stopped.")
finally:
    consumer.close()
    cursor.close()
    conn.close()