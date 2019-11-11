######## -*- coding: utf-8 -*-######
####################################
##### PROVA TECNICA - SEMANTIX #####
####### Cristina Weingartner #######
####################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Classe de Dados - leitura de logs
class DadosIO:
    _spark = None

    def __init__(self, job):
        if DadosIO._spark is None:
            DadosIO._spark = SparkSession.builder.appName(job).getOrCreate()

    # Arquivo com requisicoes HTTP para o servidor da NASA Kennedy Space​ ​Center - Julho
    def nasa_log_jul(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'false') \
            .option('delimiter', ' ') \
            .option('inferSchema', 'true') \
            .load('/Users/cristina.cruz/Desktop/prova/NASA_access_log_Jul95')

    # Arquivo com requisicoes HTTP para o servidor da NASA Kennedy Space​ ​Center - Agosto
    def nasa_log_ago(self):
        return DadosIO._spark.read \
            .format('com.databricks.spark.csv') \
            .option('header', 'false') \
            .option('delimiter', ' ') \
            .option('inferSchema', 'true') \
            .load('/Users/cristina.cruz/Desktop/prova/NASA_access_log_Aug95')

# Classe para chamada de funcoes
class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._nasa_log_jul = _dados_io.nasa_log_jul()
        self._nasa_log_ago = _dados_io.nasa_log_ago()

    def gera_regras(self):

        # Tratamento deo arquivo de log de julho
        log_jul = tratamento_log(self._nasa_log_jul)

        # Tratamento do arquivo de log de agosto
        log_ago = tratamento_log(self._nasa_log_ago)

        # Union das duas logs
        union_log = union_logs(log_jul, log_ago)

        # Primeira questao da prova tecnica
        print('Numero de hosts unicos')
        resultado1 = hosts_unicos(union_log)
        resultado1.show(1, False)

        # Segunda questao da prova tecnica
        print('Total​ ​de​ ​erros​ ​404')
        resultado2 = erro_404(union_log)
        resultado2.show(1, False)

        # Terceira questao da prova tecnica
        print('Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​ ​404')
        resultado3 = url_erro_404(union_log)
        resultado3.show(5, False)

        # Quarta questao da prova tecnica
        print('Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia')
        resultado4 = erro_404_dia(union_log)
        resultado4.show(100, False)

        # Quinta questao da prova tecnica
        print('O​ ​total​ ​de​ ​bytes​ ​retornados')
        resultado5 = retorno_bytes(union_log)
        resultado5.show(1, False)

        return resultado5

# Funcao de tratamento dos arquivos de log
def tratamento_log(df):
    return (df
           .select(df['_c0'].alias('host'),
                   f.concat(df['_c3'], f.lit(' '), df['_c4']).alias('date'),
                   df['_c5'].alias('requisicao'),
                   f.split(df['_c5'], ' ').alias('requisicao_tratado'),
                   df['_c6'].alias('retorno_http'),
                   df['_c7'].alias('bytes')
                   )
           .withColumn('url', f.concat(f.col('host'), f.col('requisicao_tratado').getItem(1)))
           ).drop('requisicao_tratado')

# Funcao que realiza o union entre as logs de julho e agosto
def union_logs(df1, df2):
    return (df1
            .union(df2)
            )

# Funcao para contar o numero de hosts unicos
def hosts_unicos(df):
    df1 = (df
           .select(df['host']
                   )
           ).distinct()
    df2 = (df1
           .select(df1['host']
                   )
           .agg(f.count('host').alias('num_hosts_unicos'))
           )
    return df2

# Funcao para contar o total de requisicoes com erro 404
def erro_404(df):
    df1 = (df
           .filter(df['retorno_http'] == '404')
           )
    df2 = (df1
           .select(df1['retorno_http']
                   )
           .agg(f.count('retorno_http').alias('tot_erro_404'))
           )
    return df2

# Funcao que retorna as 5 URL's que mais causaram o erro 404
def url_erro_404(df):
    df1 = (df
           .filter(df['retorno_http'] == '404')
           )
    df2 = (df1
           .select(df1['url'],
                   df1['retorno_http']
                   )
           .groupBy('url')
           .agg(f.count('retorno_http').alias('tot_erro_404'))
           .sort(f.col('tot_erro_404').desc())
           .limit(5))
    return df2

# Funcao que retorna a quantidade de erros 404 por dia
def erro_404_dia(df):
    df1 = (df
           .filter(df['retorno_http'] == '404')
           )
    df2 = (df1
           .select(f.substring(df1['date'], 2, 11).alias('date'),
                   df1['retorno_http']
                   )
           .groupBy('date')
           .agg(f.count('retorno_http').alias('tot_erro_404'))
           .sort(f.col('date').asc()))
    return df2

# Funcao que retorna o total de bytes
def retorno_bytes(df):
    df1 = (df
           .select(df['bytes'])
           .agg(f.sum('bytes').alias('tot_bytes')))
    return df1

_dados_io = DadosIO('PROVA TECNICA')
_gerenciador = Gerenciador(_dados_io)
df_prova = _gerenciador.gera_regras()