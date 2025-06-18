import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Match database connection details from .env file
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

# Initialize Spark Session with PostgreSQL Package
# Using 'org.postgresql:postgresql' for the JDBC driver package.
spark = SparkSession.builder \
    .appName('Youtube Data Transformation & Load') \
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0') \
    .getOrCreate()

try:
    # Load JSON data into a temporary Spark SQL table
    # 'multiline' option is useful for JSON files where each record spans multiple lines.
    df_raw = spark.read.option('multiline', 'true').json('/home/luxde/youtube/youtube_videos_raw.json')
    df_raw.createOrReplaceTempView('youtube_videos_raw')

    # Transform and enrich data using Spark SQL
    # Corrected the CASE statement to ensure proper syntax.
    # CAST functions are used to ensure correct data types for numerical operations.
    # Performance class is derived based on view counts.
    df_transformed = spark.sql('''
        SELECT
            videoId,
            title,
            publishedAt,
            CAST(viewCount AS INT) AS viewCount,
            CAST(likeCount AS INT) AS likeCount,
            CAST(commentCount AS INT) AS commentCount,
            YEAR(TO_TIMESTAMP(publishedAt)) AS year,
            MONTH(TO_TIMESTAMP(publishedAt)) AS month,
            CASE
                WHEN viewCount > 100000000 THEN 'viral'
                WHEN viewCount > 50000000 THEN 'high' -- Corrected: removed trailing comma
                ELSE 'normal'
            END AS performance_class
        FROM youtube_videos_raw
        WHERE viewCount IS NOT NULL
        ORDER BY viewCount DESC
    ''')

    # Display the first few rows of the transformed DataFrame
    df_transformed.show()

    # PostgreSQL connection settings using .env variables
    jdbc_url = f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    connection_properties = {
        'user': POSTGRES_USER, # Corrected: 'user' (singular)
        'password': POSTGRES_PASSWORD,
        'driver': 'org.postgresql.Driver' # Corrected: capitalized 'D' for Driver
    }

    # Save transformed data to PostgreSQL
    # 'append' mode will add new rows to the existing table.
    # Consider 'overwrite' if you want to replace the entire table each run.
    df_transformed.write.jdbc(
        url=jdbc_url,
        table='youtube_videos_enriched', # The table name in your PostgreSQL database
        mode='append', # Options: 'append', 'overwrite', 'ignore', 'error'
        properties=connection_properties
    )

    print('Data transformed and loaded successfully to PostgreSQL!')

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the SparkSession to release resources
    if spark:
        spark.stop()
        print('SparkSession stopped.')

