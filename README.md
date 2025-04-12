
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
user_1,Classical,16,1
user_10,Pop,18,1
user_11,Classical,13,1
user_12,Classical,13,1
user_13,Pop,21,1
user_14,Classical,13,1
user_15,Pop,15,1
user_16,Classical,17,1
user_17,Hip-Hop,12,1
...
```

---

### 2️⃣ Average Listen Time per Song

📁 Output: `output/avg_listen_time_per_song/`

```
793293ab-eda3-40ed-9ad8-74b2b4aff636,197.11111111111111
75d605c9-f878-4275-a6ab-96732750493f,137.6
fb738676-3c04-44a8-a496-6f0eeb1e509c,134.77777777777777
231f0c55-29d6-43d6-834a-d5b132db529c,134.0
19f5efd9-de2c-438d-8831-ff505117e0d7,127.15384615384616
7ef43e68-fb8d-444f-90d3-f4934c40a6dd,147.15384615384616
7155d783-e35d-4e02-84f0-b245e5aa1e5d,107.0909090909091
52855f22-3641-498b-b871-86d0fa77c5ee,181.375
e5e47066-5d2e-4a00-b380-8f5a16744b6e,145.5
5bb3fbe5-64c4-4f59-9970-dec7696a2281,178.0
...
```

---

### 3️⃣ Top 10 Most Played Songs This Week

📁 Output: `output/top_songs_this_week/`

```
7ca0b367-918d-4b42-98c5-d125c22e66ff,16
7d4d2ef5-cefb-4fd2-b9b3-b4141f732081,16
1b559456-b659-48ef-9b4c-6c3d1dc47a2b,15
7c8849ef-e3a2-4713-8721-aece9261651e,15
52855f22-3641-498b-b871-86d0fa77c5ee,15
bffa07b4-439d-4f9a-b9b1-ae1037e0a9fe,14
f730ec6f-a6de-4745-9340-bd091e6e123f,14
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
user_14,12
user_19,7
user_18,9
user_13,13
user_7,17
user_5,21
user_9,10
user_20,14
user_4,16
...
```

---

### 7️⃣ Enriched Logs (Logs + Metadata Joined)

📁 Output: `output/enriched_logs/`

```
"song_id":"ea29b646-ae77-440f-96cf-dfc0837340cd","user_id":"user_1","timestamp":"2025-03-21T13:53:00.000-04:00","duration_sec":265,"title":"Song 65","artist":"Artist 7","genre":"Classical","mood":"Energetic"}
{"song_id":"597b9b32-9c13-43d9-b727-5da2600cab9c","user_id":"user_2","timestamp":"2025-03-23T03:28:00.000-04:00","duration_sec":34,"title":"Song 47","artist":"Artist 18","genre":"Hip-Hop","mood":"Sad"}
{"song_id":"a757d7f3-174b-4e84-99e2-286fa39c33e4","user_id":"user_2","timestamp":"2025-03-25T12:48:00.000-04:00","duration_sec":155,"title":"Song 24","artist":"Artist 6","genre":"Classical","mood":"Energetic"}
{"song_id":"e8a625f0-18be-4410-8121-540f7d8c46e2","user_id":"user_13","timestamp":"2025-03-21T11:35:00.000-04:00","duration_sec":83,"title":"Song 54","artist":"Artist 1","genre":"Pop","mood":"Energetic"}
{"song_id":"c5b4a46a-1749-4e20-a5e1-955c24de1d70","user_id":"user_6","timestamp":"2025-03-23T09:17:00.000-04:00","duration_sec":221,"title":"Song 77","artist":"Artist 15","genre":"Pop","mood":"Chill"}
{"song_id":"c721239e-e84d-48a2-9bc2-e7bfce4d4eea","user_id":"user_1","timestamp":"2025-03-22T11:49:00.000-04:00","duration_sec":150,"title":"Song 4","artist":"Artist 10","genre":"Jazz","mood":"Energetic"}
{"song_id":"bffa07b4-439d-4f9a-b9b1-ae1037e0a9fe","user_id":"user_6","timestamp":"2025-03-24T23:25:00.000-04:00","duration_sec":131,"title":"Song 43","artist":"Artist 6","genre":"Rock","mood":"Happy"}
{"song_id":"e44e401b-0dff-4f3f-9153-5f1afa0a72fc","user_id":"user_13","timestamp":"2025-03-24T01:22:00.000-04:00","duration_sec":142,"title":"Song 97","artist":"Artist 13","genre":"Pop","mood":"Chill"}
{"song_id":"a32fec3a-36b9-42df-9518-326cbf9bdbba","user_id":"user_19","timestamp":"2025-03-27T13:09:00.000-04:00","duration_sec":101,"title":"Song 6","artist":"Artist 18","genre":"Pop","mood":"Energetic"}
{"song_id":"b7466efc-9414-40db-9682-9f8d92c78480","user_id":"user_14","timestamp":"2025-03-26T01:23:00.000-04:00","duration_sec":103,"title":"Song 72","artist":"Artist 2","genre":"Jazz","mood":"Happy"}
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

