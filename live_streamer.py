import time
import json
import re
import requests
from confluent_kafka import Producer

# Target skills to track
TARGET_SKILLS = [
    "Python", "Java", "Docker", "Kubernetes", "PySpark",
    "Kafka", "PostgreSQL", "AWS", "React", "Spring Boot",
    "FastAPI", "Golang", "TypeScript", "Node.js", "Redis", "GCP", "Azure"
]

# Keywords that confirm this is an engineering/data/software posting
TECH_TITLE_KEYWORDS = [
    "engineer", "developer", "architect", "data", "software",
    "backend", "frontend", "fullstack", "devops", "cloud", "sre", "ml"
]

# Configure Kafka Producer
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")

def fetch_and_stream_live_jobs():
    url = "https://remoteok.com/api"
    headers = {"User-Agent": "TrendStream-Live-Data-Pipeline/1.0"}
    
    print("🌐 Fetching live job feeds from RemoteOK API...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ API returned status: {response.status_code}")
            return
        
        jobs = response.json()
        job_listings = [j for j in jobs if isinstance(j, dict) and "position" in j]

        print(f"✅ Found {len(job_listings)} live postings. Filtering tech listings...\n")

        for job in job_listings:
            title = job.get("position", "")
            title_lower = title.lower()

            # 1. Filter: Ensure it's a technical role
            if not any(k in title_lower for k in TECH_TITLE_KEYWORDS):
                continue

            tags = [str(t).strip().lower() for t in job.get("tags", [])]
            description = job.get("description", "").lower()

            # 2. Extract skills from structured tags or verified keyword matches
            matched_skills = []
            for skill in TARGET_SKILLS:
                s_lower = skill.lower()
                
                # Check tags first (most accurate)
                if s_lower in tags or (s_lower == "golang" and "go" in tags):
                    matched_skills.append(skill)
                    continue

                # Whole-word regex on description (exclude short ambiguous terms from free text)
                if s_lower != "golang":
                    pattern = r'\b' + re.escape(s_lower) + r'\b'
                    if re.search(pattern, description):
                        matched_skills.append(skill)

            if matched_skills:
                payload = {
                    "job_title": title,
                    "company": job.get("company", "Tech Company"),
                    "required_skills": matched_skills,
                    "timestamp": int(time.time()),
                    "is_live": True
                }

                # Publish to Kafka topic 'tech-jobs'
                producer.produce(
                    'tech-jobs',
                    key=title.encode('utf-8'),
                    value=json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                
                print(f"📡 [TECH JOB MATCH] {title} @ {job.get('company')} ➔ Skills: {matched_skills}")
                time.sleep(1)

        producer.flush()

    except Exception as e:
        print(f"⚠️ Error fetching live jobs: {e}")

if __name__ == "__main__":
    print("🚀 [TrendStream] Starting Filtered Live Tech Ingestion...\n")
    while True:
        fetch_and_stream_live_jobs()
        print("\n⏳ Sleeping 60s before next live poll...")
        time.sleep(60)