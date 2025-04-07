import pandas as pd
import random
import uuid

titles = [f"Song {i}" for i in range(1, 101)]
artists = [f"Artist {i}" for i in range(1, 21)]
genres = ['Pop', 'Rock', 'Jazz', 'Hip-Hop', 'Classical']
moods = ['Happy', 'Sad', 'Energetic', 'Chill']

songs_data = []

for i in range(100):
    songs_data.append({
        'song_id': str(uuid.uuid4()),
        'title': titles[i],
        'artist': random.choice(artists),
        'genre': random.choice(genres),
        'mood': random.choice(moods)
    })

df_songs = pd.DataFrame(songs_data)
df_songs.to_csv('songs_metadata.csv', index=False)

