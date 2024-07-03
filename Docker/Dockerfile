FROM python:3.8
RUN pip install boto3

WORKDIR /app
COPY enviarCSV.py /app

VOLUME /data
COPY arquivos/movies.csv /data/movies.csv
COPY arquivos/series.csv /data/series.csv

CMD ["python", "enviarCSV.py"]