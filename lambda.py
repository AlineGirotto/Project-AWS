import boto3
from botocore.exceptions import ClientError
import os
import requests
import json
import csv
from datetime import datetime 

bucket_name = 'projeto2aline'
api_key = os.environ.get("API_key")
data_atual = datetime.now()
data_formatada = data_atual.strftime("%Y/%m/%d")
collection = []

def lambda_handler(event, context):
    try:
        credits_movies()
        filtro = filtra_Filmes()
        all_movies(filtro)
    except Exception as e:
        print(f"Erro no processo de leituras e escritas: {e}")

def upload_file(file_name,caminho):
    s3_client = boto3.client('s3')
    try:
        #s3_client.put_object(Body=file_name, Bucket=bucket_name, Key=caminho)
        print("Arquivo enviado com sucesso ao S3!")
    except ClientError as e:
        print(e)
        return False
    return True

def filtra_Filmes():
    movies_id = []
    try:
        movies_id = [item['id'] for filme in collection for item in filme['cast']]
        print("Sucesso ao filtrar os filmes da atriz")
        return movies_id
    except Exception as e:
        print(f"Erro ao filtrar os filmes da atriz: {e}")

def credits_movies():
    global collection
    caminho = f'Raw/TMDB/JSON/{data_formatada}/movies_credits.json'
    try:  
        res = requests.get(f'https://api.themoviedb.org/3/person/1245/movie_credits?api_key={api_key}')
        if res.status_code == 200:
            collection.append(res.json())
        else:
            print(f'Erro na requisição à API credits_movies: {res.status_code}')
    except Exception as e:
        print(f"Erro ao ler a API credits_movies: {e}")
    file = json.dumps(collection, ensure_ascii=False, indent=4)
    upload_file(file, caminho)
    
def all_movies(filtro):
    resultado = []
    caminho = f'Raw/TMDB/JSON/{data_formatada}/filmes_CSV.json'
    for id in filtro:
        try:
            res = requests.get(f'https://api.themoviedb.org/3/movie/{id}?api_key={api_key}&language=pt-BR')
            if res.status_code == 200:
                resultado.append(res.json())
            else:
                print(f'Erro na requisição à API movies_csv: {res.status_code}')
        except Exception as e:
            print(f"Erro ao ler a API movies_csv: {e}")
    file = json.dumps(resultado, ensure_ascii=False, indent=4)
    upload_file(file, caminho)