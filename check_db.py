import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="tech_trends",
    user="admin",
    password="password123"
)
cursor = conn.cursor()

cursor.execute("SELECT skill, mention_count, last_updated FROM tech_skill_counts ORDER BY mention_count DESC;")
rows = cursor.fetchall()

print("\n📊 --- LIVE TECH TREND COUNTS IN POSTGRESQL ---")
print(f"{'Skill':<20} | {'Mentions':<10} | {'Last Updated'}")
print("-" * 55)
for row in rows:
    print(f"{row[0]:<20} | {row[1]:<10} | {row[2]}")
print("-" * 55 + "\n")

cursor.close()
conn.close()