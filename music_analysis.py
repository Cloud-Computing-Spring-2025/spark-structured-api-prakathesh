from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalytics").getOrCreate()

logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Join logs with metadata
enriched = logs.join(songs, "song_id")
enriched.write.mode("overwrite").json("output/enriched_logs/")

# 1. User's Favorite Genre
user_genre_counts = enriched.groupBy("user_id", "genre").count()
window_genre = Window.partitionBy("user_id").orderBy(col("count").desc())
fav_genre = user_genre_counts.withColumn("rank", row_number().over(window_genre)).filter("rank = 1")
fav_genre.write.mode("overwrite").csv("output/user_favorite_genres/")

# 2. Average Listen Time per Song
avg_listen = logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_listen_time"))
avg_listen.write.mode("overwrite").csv("output/avg_listen_time_per_song/")

# 3. Top 10 Songs This Week
week_start = "2025-03-20"
week_end = "2025-03-27"
top_songs = logs.filter((col("timestamp") >= week_start) & (col("timestamp") <= week_end)) \
                .groupBy("song_id") \
                .count().orderBy(desc("count")) \
                .limit(10)
top_songs.write.mode("overwrite").csv("output/top_songs_this_week/")

# 4. Recommend Happy Songs to Sad Listeners
sad_listeners = enriched.filter(col("genre") == "Sad") \
                        .groupBy("user_id", "genre").count() \
                        .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(desc("count")))) \
                        .filter("rank = 1 AND genre = 'Sad'") \
                        .select("user_id")

happy_songs = songs.filter(col("mood") == "Happy").select("song_id", "title")

recommend = sad_listeners.join(logs, "user_id", "left_anti") \
                         .crossJoin(happy_songs) \
                         .limit(3)  # adjust logic if needed

recommend.write.mode("overwrite").csv("output/happy_recommendations/")

# 5. Genre Loyalty Score
total_plays = enriched.groupBy("user_id").count()
top_genre_plays = user_genre_counts.withColumn("rank", row_number().over(window_genre)) \
                                   .filter("rank = 1") \
                                   .select("user_id", col("count").alias("top_genre_count"))
loyalty = total_plays.join(top_genre_plays, "user_id") \
                     .withColumn("loyalty_score", col("top_genre_count") / col("count")) \
                     .filter("loyalty_score > 0.8")
loyalty.write.mode("overwrite").csv("output/genre_loyalty_scores/")

# 6. Night Owl Users
night_users = logs.withColumn("hour", hour(col("timestamp"))) \
                  .filter((col("hour") >= 0) & (col("hour") <= 5)) \
                  .groupBy("user_id").count()
night_users.write.mode("overwrite").csv("output/night_owl_users/")
