## Passo a passo do projeto:

✔ Iniciei a leitura do material disponibilizado na Udemy, percebi passos a serem seguidos para melhor implementar o objetivo, dentre eles estão: </br>
    ➡ Ler arquivos CSV. </br>
    ➡ Consumir dados da API TMDB com o Lambda.</br>
    ➡ Selecionar qual o tipo de análise a ser feito.</br>
    ➡ Fazer os processos de ETL para atingir a análise.</br>
    ➡ Por fim gerar um dashboard com as informações analisadas.</br>

✔ Após fazer o cadastro na API TMBD, testei o script do laboratório para averiguar como funciona a API e como se comunicar com ela. </br>

✔ Sobre a análise escolhida, o tema será sobre a atriz Scarlett Johansson:</br>
    ➡ Filmes mais reconhecidos.</br>
    ➡ Filmes mais avaliados.</br>
    ➡ Filmes com maior receita.</br>
    ➡ Filmes com maior gasto.</br>
    ➡ Filmes mais recentes</br>
    ➡ Principais gêneros dos filmes.</br>
    ➡ Popularidade da atriz.</br>
    ➡ Média de avaliação dos gêneros.</br>

✔ Próximo passo é criar um bucket no S3 para enviar os arquivos locais ao repositório, nomeei o mesmo de projeto2aline e deixei o acesso público, na qual irei criar uma pasta para cada etapa do projeto (raw, trusted e refined) e anexar as informações necessárias.</br>

✔ Construi um diretório para o ambiente do Docker: </br>
    ➡ enviarCSV.py: o arquivo em python para enviar os arquivos locais para o bucket S3.</br>
    ➡ Dockerfile: com as especificações do container e arquivos.</br>
    ➡ arquivos: arquivos csv para enviar ao Docker, salve dentro da pasta os arquivos movies.csv e series.csv *(não os enviei ao git pois excede o tamanho do upload)*</br>
    ➡ Para rodar o docker use os comandos: </br>
        - docker build -t enviarcsvdocker .</br>
       - docker run -it enviarcsvdocker

<b>Vale ressaltar que ao enviar o código ao GIT, eu excluo minhas chaves de acesso a AWS para não comprometer minha conta e projeto, mas basta modificar as credenciais no arquivo python para conseguir enviar os arquivos do container Docker para a AWS S3.</b>

✔ Seguindo a etapa 2 do projeto, criei a função no Lambda chamada readApiTMDB, com Python 3.7 e defini a variável ambiente chamada "API_key" que consta a chave de acesso a Api do TMDB. Com a função no lambda, criei o arquivo localmente chamado lambda.py para trabalhar no script do lambda e anexá-lo posteriormente a AWS. Obtive um problema com acesso ao S3, para resolver alterei a regra(Role) da função dando permissão ao S3, aumentei o timeout e memoria da função e vinculei um layer já criado anteriormente do Pandas para auxiliar no script.

O script foi escrito de uma forma, mas posteriormente reformulado para atender melhor ao refinamento, no momento ela busca em uma API todos os filmes na qual a atriz tem créditos (alguma participação) e com os IDs desses filmes ele busca em outra API detalhes sobre cada um desses filmes.</br>

✔ Seguindo a etapa 3 do projeto, criei dois jobs spark para a camada trusted, job_CSV_Projeto para fazer o tratamento dos dados do arquivo CSV do S3 e o Job_TMDB_Projeto para lidar com os dados vindos da API no S3. Em ambos utilizei o Job parameters para definir as keys de input e output ao S3. Com eles criados e os scripts fazendo a limpeza e manipulação dos dados necessários, criei posteriormente os crawlers para armazenar os dados no catálogo do Glue de forma automática.</br>
       Job_csv_crawler: Envia os dados dos filmes do csv limpos para o catálogo.</br>
       Job_csv_generos: Envia os dados dos generos do csv para o catálogo.</br>
       Job_csv_genresmovies: Envia os dados da relação de generos e filmes para o catálogo.</br>
       Job_TMDB_movies: Envia os filmes do json do tmdb para o catálogo.<br/>
       Job_TMDB_genres: Envia os generos do json do tmdb para o catálogo.<br/>
       Job_TMDB_genremovie: Cria a relação dos filmes e generos do tmdb no catálogo.</br>

✔ Devido os imprevistos com o acesso a AWS, instalei localmente uma imagem do glue para continuar o projeto (<a>https://github.com/soumilshah1995/run-aws-glue-locally-docker</a>).<br/>

✔ Na etapa Refined, optei por criar um job apenas para facilitar o desenvolvimento do código, ele é chamado de Job_Refined_Projeto e conta com as mesmas configurações dos demais jobs. Ele vai ler as informações das tabelas criadas na etapa trusted e fazer o refinamento necessário para atingir os objetivos do QuickSight. Da mesma forma que a etapa trusted, eu criei crawlers para enviar os dados ao catálogo do glue, os dados refinados estão na camada refined do S3.</br>
Criei o Database dl_refined_zone e com os crawlers fui adicionando as tabelas:</br>
       Job_Refined_Movies: Cria a tabela movies com os dados dos filmes.<br/>
       Job_Refined_Values: Cria a tabela values com os dados referentes aos valores dos filmes.</br>
       Job_Refined_Genres: Cria a tabela com os gêneros dos filmes.</br>
       Job_Refined_GenreMovie: Cria a tabela de relação entre os filmes e os gêneros.</br>

✔ Na etapa final do projeto, criei uma pasta no S3 chamada Queries, para poder armazenas as queries feitas pelo Athena. Foi a etapa que mais tive dificuldade e por conta dos prazos apertados tive que repensar na minha análise. Durante a construção do dashboard no QuickSight, tive vários problemas com o data set, onde as tabelas não estavam se relacionando bem. Mas aos poucos com alguns tutoriais e o material disponível na Udemy consegui construir um pequeno dashboard com algumas análises, utilizando alguns filtros e parâmetros nos dados analisados. 