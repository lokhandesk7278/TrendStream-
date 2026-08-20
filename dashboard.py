import time
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="TrendStream Analytics", page_icon="📈", layout="wide")
st.title("⚡ TrendStream: Live Tech Skill Demand Dashboard")
st.caption("Real-time distributed data pipeline aggregating Kafka event streams into PostgreSQL")

placeholder = st.empty()

def fetch_data():
    conn = psycopg2.connect(
        host="localhost",
        port="5433",
        database="tech_trends",
        user="admin",
        password="password123"
    )
    df = pd.read_sql_query(
        "SELECT skill, mention_count, last_updated FROM tech_skill_counts ORDER BY mention_count DESC;", 
        conn
    )
    conn.close()
    return df

iteration = 0

while True:
    try:
        df = fetch_data()
        iteration += 1
        
        with placeholder.container():
            col1, col2, col3 = st.columns(3)
            total_mentions = int(df["mention_count"].sum()) if not df.empty else 0
            top_skill = df.iloc[0]["skill"] if not df.empty else "N/A"
            top_skill_count = int(df.iloc[0]["mention_count"]) if not df.empty else 0
            
            col1.metric("Total Skill Mentions", f"{total_mentions:,}")
            col2.metric("Top Trending Skill", top_skill)
            col3.metric("Top Skill Volume", f"{top_skill_count:,}")

            st.write("---")

            if not df.empty:
                # Plot interactive horizontal bar chart with a dynamic unique key
                fig = px.bar(
                    df,
                    x="mention_count",
                    y="skill",
                    orientation='h',
                    color="mention_count",
                    color_continuous_scale="Blues",
                    labels={"mention_count": "Total Mentions", "skill": "Skill / Tech"},
                    title="🔥 Live Tech Skill Popularity (Auto-Updating)"
                )
                fig.update_layout(yaxis=dict(autorange="reversed"), height=450)
                st.plotly_chart(fig, use_container_width=True, key=f"tech_trend_chart_{iteration}")

                # Raw Data Table
                with st.expander("🔍 View Raw PostgreSQL Stream Records"):
                    st.dataframe(df, use_container_width=True)
            else:
                st.info("Awaiting live stream data... Please ensure `mock_streamer.py` and `stream_processor.py` are running.")

        time.sleep(1)
        
    except Exception as e:
        st.error(f"Error fetching records: {e}")
        time.sleep(2)