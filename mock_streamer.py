import json
import time
import random
from kafka import KafkaProducer

# Initialize Kafka producer connected to local broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

skills_pool = [
    "Spring Boot", "Kafka", "Docker", "PySpark", 
    "PostgreSQL", "Java", "Kubernetes", "AWS", "Python", "React"
]
job_titles = [
    "Backend Engineer", "Data Engineer", 
    "Systems Engineer", "Cloud Architect", "Full Stack Developer"
]

def generate_job_event():
    return {
        "job_title": random.choice(job_titles),
        "required_skills": random.sample(skills_pool, k=random.randint(1, 4)),
        "timestamp": int(time.time())
    }

print("🚀 [TrendStream] Streaming job events to Kafka topic 'tech-jobs'...\n")

try:
    while True:
        event = generate_job_event()
        producer.send('tech-jobs', value=event)
        print(f"✅ Sent: {event['job_title']} -> {event['required_skills']}")
        time.sleep(1)  # Simulates continuous 1 event/sec ingestion
except KeyboardInterrupt:
    print("\n🛑 Stream stopped.")