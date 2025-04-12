
# 🎵 Music Listening Behavior Analysis using Spark Structured API

This project analyzes fictional user listening behavior on a music streaming platform using **Apache Spark (PySpark Structured APIs)**. It processes user play logs and song metadata to uncover insights about user preferences, song popularity, engagement patterns, and personalized recommendations.

---

## 📂 Dataset Description

### 📄 `listening_logs.csv`
Log of each time a user played a song.

| Column         | Description                                     |
|----------------|-------------------------------------------------|
| `user_id`      | Unique ID of the user                           |
| `song_id`      | Unique ID of the song                           |
| `timestamp`    | Timestamp when the song was played              |
| `duration_sec` | Duration (in seconds) the song was listened to  |

---

### 📄 `songs_metadata.csv`
Metadata for all songs in the catalog.

| Column     | Description                                  |
|------------|----------------------------------------------|
| `song_id`  | Unique ID of the song                        |
| `title`    | Song title                                   |
| `artist`   | Artist name                                  |
| `genre`    | Genre (Pop, Rock, Jazz, etc.)                |
| `mood`     | Mood (Happy, Sad, Energetic, Chill, etc.)    |

---

## ✅ Tasks Completed & Sample Outputs

All results are saved in the `output/` directory.

---

### 1️⃣ Each User’s Favorite Genre

📁 Output: `output/user_favorite_genres/`

```
user_id,genre,play_count
user_1,Classical,9
user_10,Classical,9
user_11,Hip-Hop,8
user_12,Jazz,8
user_13,Pop,5
...
```

---

### 2️⃣ Average Listen Time per Song

📁 Output: `output/avg_listen_time_per_song/`

```
song_id,avg_duration
song_19,133.89
song_47,167.57
song_54,185.78
song_100,198.5
song_84,214.0
...
```

---

### 3️⃣ Top 10 Most Played Songs This Week

📁 Output: `output/top_songs_this_week/`

```
song_id,plays
song_36,12
song_38,10
song_71,10
song_52,9
song_78,8
...
```

---

### 4️⃣ Recommend “Happy” Songs to “Sad” Listeners

📁 Output: `output/happy_recommendations/`

```
user_id,song_id,sad_count,title
user_14,song_75,3,Title_song_75
user_14,song_97,3,Title_song_97
user_14,song_6,3,Title_song_6
```

---

### 5️⃣ Genre Loyalty Score > 0.8

📁 Output: `output/genre_loyalty_scores/`

```
message
No users found with genre loyalty score above 0.8.
```

---

### 6️⃣ Night Owl Users (12AM to 5AM)

📁 Output: `output/night_owl_users/`

```
user_id
user_14
user_22
user_47
user_19
user_10
user_50
...
```

---

### 7️⃣ Enriched Logs (Logs + Metadata Joined)

📁 Output: `output/enriched_logs/`

```
song_id,user_id,timestamp,duration_sec,title,artist,genre,mood
song_43,user_45,2025-03-26T02:53:00Z,219,Title_song_43,Artist_4,Hip-Hop,Chill
song_99,user_16,2025-03-25T17:27:00Z,59,Title_song_99,Artist_13,Classical,Sad
song_78,user_23,2025-03-25T08:36:00Z,274,Title_song_78,Artist_7,Hip-Hop,Happy
...
```

---

## 💻 How to Run the Project

### 1. Generate the datasets

```bash
python generate_listening_logs.py
python generate_songs_metadata.py
```

### 2. Run the full analysis

```bash
spark-submit analysis.py
```

> 📁 All outputs will be saved in the `output/` folder.

---

## ⚠️ Errors & Fixes

### ❌ `Window is not defined`
**Fix:** Added import:
```python
from pyspark.sql.window import Window
```

---

### ❌ `Fail to recognize 'yyyy-ww' pattern`
**Fix:** Used supported functions:
```python
from pyspark.sql.functions import year, weekofyear
week_logs = logs.filter((year("timestamp") == 2025) & (weekofyear("timestamp") == 13))
```

---

### ❌ Multiple part files
**Fix:** Used `.coalesce(1)` before writing:
```python
df.coalesce(1).write.mode("overwrite").csv(...)
```

---

### ❌ Empty results for loyalty score
**Fix:** Saved message in CSV if no users met the threshold:
```python
message_df = spark.createDataFrame([Row(message="No users found with genre loyalty score above 0.8.")])
```

---

## 🗂 Folder Structure

```
.
├── generate_listening_logs.py
├── generate_songs_metadata.py
├── analysis.py
├── README.md
└── output/
    ├── user_favorite_genres/
    ├── avg_listen_time_per_song/
    ├── top_songs_this_week/
    ├── happy_recommendations/
    ├── genre_loyalty_scores/
    ├── night_owl_users/
    └── enriched_logs/
```

---

