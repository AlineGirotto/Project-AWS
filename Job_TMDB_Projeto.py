import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, explode, concat_ws
import pandas as pd

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT', 'S3_OUTPUT'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
source_file = args['S3_INPUT']
target_path = args['S3_OUTPUT']

#Le os dados do json TMDB do S3 e faz a leitura dos dados da tabela generos para associar aos dos filmes do json
df = spark.read.option("multiline", "true").option("inferSchema", "False").json(source_file + "/all_movies.json")


#Filtra por filmes já lançados e com regitro imdb_id
only_released = df.filter((col("status") == "Released") & col("imdb_id").isNotNull())


#Filtra as colunas necessárias para a análise
columns = ["imdb_id", "original_title", "budget", "popularity", "revenue", "vote_average", "vote_count", "release_date"]
df_select = only_released.select(columns)


#Cria as tabelas de genero e a tabelas generos em relação aso filmes
def genresTable(df):
    genres = (only_genders.select("genre.id", "genre.name").distinct().withColumnRenamed("id", "genero_id").withColumnRenamed("name", "genero_name"))
    return genres

def genres_moviesTable(df):
    genres_movies = (only_genders.withColumn("id", col("genre.id")).drop("genre"))
    return genres_movies
    
only_genders = only_released.select(col("imdb_id"), explode(col("genres")).alias("genre"))
genre_table_tmdb = genresTable(only_genders)
genre_movies_table = genres_moviesTable(only_genders)

df_select = DynamicFrame.fromDF(df_select, glueContext, "df_select")
genre_table_tmdb = DynamicFrame.fromDF(genre_table_tmdb, glueContext, "genre_table_tmdb")
genre_movies_table = DynamicFrame.fromDF(genre_movies_table, glueContext, "genre_movies_table")

glueContext.write_dynamic_frame.from_options(
    frame= df_select,
    connection_type = 's3',
    connection_options = {'path': target_path + 'TMDB/2023/11/17/movies'},
    format = 'parquet'
)

glueContext.write_dynamic_frame.from_options(
    frame= genre_movies_table ,
    connection_type = 's3',
    connection_options = {'path': target_path + 'TMDB/2023/11/17/movies_genres_data'},
    format = 'parquet'
)

glueContext.write_dynamic_frame.from_options(
    frame= genre_table_tmdb ,
    connection_type = 's3',
    connection_options = {'path': target_path + 'TMDB/2023/11/17/genres_data'},
    format = 'parquet'
)
        
job.commit()