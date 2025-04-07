import pandas as pd
import random
from datetime import datetime, timedelta
import uuid

# Load song IDs
songs_df = pd.read_csv('songs_metadata.csv')
song_ids = songs_df['song_id'].tolist()

user_ids = [f"user_{i}" for i in range(1, 21)]

logs = []

# Simulate 1000 listening events
for _ in range(1000):
    user = random.choice(user_ids)
    song = random.choice(song_ids)
    play_time = datetime(2025, 3, random.randint(20, 27), random.randint(0, 23), random.randint(0, 59), 0)
    duration = random.randint(30, 300)  # seconds
    logs.append({
        'user_id': user,
        'song_id': song,
        'timestamp': play_time.strftime('%Y-%m-%d %H:%M:%S'),
        'duration_sec': duration
    })

df_logs = pd.DataFrame(logs)
df_logs.to_csv('listening_logs.csv', index=False)
