import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT', 'S3_OUTPUT'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
source_file = args['S3_INPUT']
target_path = args['S3_OUTPUT']

#Faz a leitura dos dados 
movies_csv = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file + 'data_CSV/']},
    "parquet"
)
genres = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file + 'genres/']},
    "parquet"
)
genres_movies_csv = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file + 'movies_genres_data/']},
    "parquet"
)
movies_tmdb = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file + 'TMDB/2023/11/17/movies/']},
    "parquet"
)
genres_movies = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file + 'TMDB/2023/11/17/movies_genres_data/']},
    "parquet"
)
genres_tmdb = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file + 'TMDB/2023/11/17/genres_data//']},
    "parquet"
)
#Padroniza os nomes das colunas
mape_columns_name = {
    "id": "imdb_id",
    "tituloPrincipal": "original_title",
    "generoArtista": "genre_act",
    "notaMedia": "vote_average",
    "numeroVotos": "vote_count",
    "anoLancamento": "yearLaunch"
}
for chave_antiga, novo_nome in mape_columns_name.items():
    movies_csv = movies_csv.rename_field(chave_antiga, novo_nome)

#Compara os dados do TMDB com o CSV, gerando uma única tabela
movies_tmdb_df = movies_tmdb.toDF()
movies_csv_df = movies_csv.toDF()
missing_imdb_ids_df = movies_csv_df.join(
    movies_tmdb_df,
    movies_csv_df['imdb_id'] == movies_tmdb_df['imdb_id'],
    'left_anti'
)
if missing_imdb_ids_df is None or missing_imdb_ids_df.isEmpty():
     print("Nada a acrecentar ao TMDB.")
else:
    movies_tmdb_df = movies_tmdb_df.union(missing_imdb_ids_df)

columns = ['imdb_id', 'original_title', 'popularity', 'vote_count', 'vote_average' , 'release_date']
joined_movies = movies_tmdb_df.select(columns)

#Gera a tabela de valores referentes aos filmes
columns = ['imdb_id', 'budget', 'revenue']
joined_movies_values = movies_tmdb_df.select(columns)

values = DynamicFrame.fromDF(joined_movies_values, glueContext, "joined_movies_values")
movies = DynamicFrame.fromDF(joined_movies, glueContext, "joined_movies")

glueContext.write_dynamic_frame.from_options(
    frame=movies,
    connection_type = 's3',
    connection_options = {'path': target_path + '/movies'},
    format = 'parquet'
)
glueContext.write_dynamic_frame.from_options(
    frame=values,
    connection_type = 's3',
    connection_options = {'path': target_path + '/values'},
    format = 'parquet'
)
glueContext.write_dynamic_frame.from_options(
    frame=genres_tmdb,
    connection_type = 's3',
    connection_options = {'path': target_path + '/genres'},
    format = 'parquet'
)
glueContext.write_dynamic_frame.from_options(
    frame=genres_movies,
    connection_type = 's3',
    connection_options = {'path': target_path + '/genres_movies'},
    format = 'parquet'
)

job.commit()