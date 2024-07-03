import os
import boto3
from botocore.exceptions import NoCredentialsError

bucket = 'projeto2aline'
s3 = boto3.client('s3', aws_access_key_id='SECRETO', aws_secret_access_key='SECRETO', aws_session_token='SECRETO')

#Le os arquivos do volume

m= os.path.join("/data", "movies.csv")
s= os.path.join("/data", "series.csv")

def movies(local_file, bucket):    
    caminho = 'Raw/Local/CSV/Movies/2023/11/14/movies.csv'
    try:
        s3.upload_file(local_file, bucket, caminho)
        print("Filmes.csv enviado com sucesso ao S3!")
        return True 
    except FileNotFoundError:
        print("O arquivo não foi encontrado.")
        return False 
    except NoCredentialsError:
        print("Credenciais AWS não disponíveis.")
        return False 

def series(local_file, bucket):    
    caminho = 'Raw/Local/CSV/Series/2023/11/14/series.csv'
    try:
        s3.upload_file(local_file, bucket, caminho)
        print("Series.csv enviado com sucesso ao S3!")
        return True 
    except FileNotFoundError:
        print("O arquivo não foi encontrado.")
        return False 
    except NoCredentialsError:
        print("Credenciais AWS não disponíveis.")
        return False 
    

movies(m, bucket)
series(s, bucket)