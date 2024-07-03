import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType
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

#Busca os dados no S3 camada Raw
df = glueContext.create_dynamic_frame.from_options(
    "s3",
        {"paths" : [source_file]},
    "csv",
        {"withHeader": True , "separator" : "|"},
    )
    
#Filtra apenas a atriz escolhida
only_Scarlett = df.filter(lambda row: row['nomeArtista'] == 'Scarlett Johansson')

#Filtra apenas os dados necessários para análise
columns = ["id", "tituloPincipal", "generoArtista", "notaMedia", "numeroVotos", "anoLancamento"]
df_select = only_Scarlett.select_fields(columns)

# Faz um filtro e constrói um DynamicFrame para os gêneros dos filmes
only_genders = SelectFields.apply(frame = only_Scarlett, paths = ['genero'])
generos_unicos = set()
for filme in only_genders.toDF().collect():
    generos_unicos.update(filme["genero"].split(','))
lista_generos_unicos = list(generos_unicos)
df_genres = DynamicFrame.fromDF(spark.createDataFrame([(i + 1, genero) for i, genero in enumerate(lista_generos_unicos)], ["ID", "genero"]), glueContext, "df_genres")

# Faz um DynamicFrame que relaciona os filmes com os gêneros
complet = only_Scarlett.toDF().toPandas()
df_movies_genres = pd.DataFrame(columns=["FilmeID", "GeneroID"])
for index, movie in complet.iterrows():
    for genre in movie["genero"].split(","):
        genre_id = df_genres.toDF().filter(f"genero = '{genre}'").select("ID").collect()
        if genre_id:
            df_movies_genres = pd.concat([df_movies_genres, pd.DataFrame({"FilmeID": [movie["id"]], "GeneroID": [genre_id[0][0]]})], ignore_index=True)

df_movies_genres_dynamic = DynamicFrame.fromDF(spark.createDataFrame(df_movies_genres), glueContext, "df_movies_genres_dynamic")

#Ajusta os tipos de dados do dataframe
df_select2 = df_select.toDF()
df_select2 = df_select.toDF().withColumn("notaMedia", col("notaMedia").cast(DoubleType())) \
                             .withColumn("numeroVotos", col("numeroVotos").cast(IntegerType())) \
                             .withColumn("anoLancamento", col("anoLancamento").cast(IntegerType()))
df_dados = DynamicFrame.fromDF(df_select2, glueContext, "df_select2")

glueContext.write_dynamic_frame.from_options(
    frame= df_dados,
    connection_type = 's3',
    connection_options = {'path': target_path + '/data_CSV'},
    format = 'parquet'
)

glueContext.write_dynamic_frame.from_options(
    frame= df_genres,
    connection_type = 's3',
    connection_options = {'path': target_path + '/genres'},
    format = 'parquet'
)

glueContext.write_dynamic_frame.from_options(
    frame= df_movies_genres_dynamic ,
    connection_type = 's3',
    connection_options = {'path': target_path + '/movies_genres_data'},
    format = 'parquet'
)
    
job.commit()