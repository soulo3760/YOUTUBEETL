import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

#load environment variables securely
load_dotenv()

#match database connection details from .env file
# Database Configuration
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
    
    
# POSTGRES CONNECTION SETTING USING . ENV VARIABLES 

JDBC_URL 
CONNECTION_PROPERTIES = {
    'USER':
    'PASSWORD':
    'DRIVER': 
}
# SAVE TRANSFORMED DATA TO POSTGRESS
df_tranformed.write.jdbc(
    url=jdbc_url
    table= 
    
)

}
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, when, to_timestamp

# Initialize Spark Session with PostgreSQL driver
spark = SparkSession.builder \
    .appName('YouTube Data Transformation') \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Load JSON data into temporary Spark table
df_raw = spark.read.option('multiline', 'true').json('##path to where your json is stored')
df_raw.createOrReplaceTempView('youtube_videos_raw')

# Transform and enrich data using Spark SQL
df_transformed = spark.sql('''
    SELECT 
        video_id,
        title,
        published_at,
        CAST(view_count AS INT) AS view_count,
        CAST(like_count AS INT) AS like_count,
        CAST(comment_count AS INT) AS comment_count,
        YEAR(TO_TIMESTAMP(published_at)) AS year,
        MONTH(TO_TIMESTAMP(published_at)) AS month,
        CASE 
            WHEN view_count > 100000 THEN 'viral'
            WHEN view_count > 5000 THEN 'high'
            ELSE 'normal'
        END AS performance_class,
        description,
        thumbnail_url,
        duration,
        channel_id,
        channel_title
    FROM youtube_videos_raw
    WHERE view_count IS NOT NULL
    ORDER BY view_count DESC
''')

# # Alternative DataFrame API version (more Pythonic)
# df_transformed = df_raw.select(
#     col('video_id'),
#     col('title'),
#     col('published_at'),
#     col('view_count').cast('int'),
#     col('like_count').cast('int'),
#     col('comment_count').cast('int'),
#     year(to_timestamp(col('published_at'))).alias('year'),
#     month(to_timestamp(col('published_at'))).alias('month'),
#     when(col('view_count') > 100000, 'viral')
#      .when(col('view_count') > 5000, 'high')
#      .otherwise('normal').alias('performance_class'),
#     col('description'),
#     col('thumbnail_url'),
#     col('duration'),
#     col('channel_id'),
#     col('channel_title')
# ).filter(col('view_count').isNotNull()).orderBy(col('view_count').desc())

# Show transformed data
df_transformed.show(10, truncate=False)
df_transformed.printSchema()

# Write to PostgreSQL
df_transformed.write \
    .format('jdbc') \
    .option('url', f'jdbc:postgresql://{db_config["host"]}:{db_config["port"]}/{db_config["database"]}') \
    .option('dbtable', 'youtube_videos') \
    .option('user', db_config['user']) \
    .option('password', db_config['password']) \
    .mode('append') \
    .save()

spark.stop()