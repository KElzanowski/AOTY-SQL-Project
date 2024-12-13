import sqlite3
import pandas as pd

# Step 1: Load the CSV
dataset_path = "aoty.csv"
dataframe = pd.read_csv(dataset_path, dtype={"rating_count": str})

# Step 2: Clean the "rating_count" column and standardize column names
dataframe["rating_count"] = pd.to_numeric(dataframe["rating_count"], errors="coerce").fillna(0)
dataframe.columns = dataframe.columns.str.lower()

# Step 3: Connect to SQLite and set up the database
connect = sqlite3.connect("aoty.db")
cursor = connect.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS Albums (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    artist TEXT,
    release_date TEXT,
    genres TEXT,
    user_score INTEGER,
    rating_count INTEGER,
    album_link TEXT
)
""")
connect.commit()

# Insert data into the database
dataframe.to_sql("Albums", connect, if_exists="replace", index=False)
print("Data has been inserted into the database.")

# Step 4: Query the top 10 albums by genre
query = """
WITH RankedAlbums AS (
    SELECT
        genres,
        title,
        user_score,
        artist,
        ROW_NUMBER() OVER (PARTITION BY genres ORDER BY user_score DESC) AS rank
    FROM Albums
)
SELECT
    genres,
    title,
    artist,
    ROUND(AVG(user_score) OVER (PARTITION BY genres), 1) AS average_score
FROM RankedAlbums
WHERE rank <= 10
ORDER BY genres, rank;
"""
result = cursor.execute(query).fetchall()

# Step 5: Export the result to Excel
columns = ["Genres", "Title", "Artist", "Average_Score"]
result_df = pd.DataFrame(result, columns=columns)
result_df.to_excel("top_albums_by_genres.xlsx", index=False)
print("Top albums by genres have been exported to an Excel file.")

# Close the connection
connect.close()