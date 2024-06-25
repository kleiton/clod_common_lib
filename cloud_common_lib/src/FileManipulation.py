import base64
import logging
import pandas as pd
import os
import shutil
import re
import unicodedata as ud
from datetime import datetime
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend


class FileManipulation:
    @staticmethod
    def create_work_dir(folder: str):
        """
        Função para criar localmente os diretórios de espelho do lake.

        Parâmetros:
            folder (str): Pasta a ser criada dentro das camadas do espelho do lake.

        Retorno:
            None
        """
        dirs = [
            f'files/raw/{folder}', f'files/trusted/{folder}', f'files/refined/{folder}',
            f'files/exploratory/{folder}', f'files/features/{folder}', f'files/sandbox/{folder}'
        ]
        for directory in dirs:
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.makedirs(directory)
            logging.info("Diretório {0} criado com sucesso!".format(directory))

    @staticmethod
    def move_file_between_layers(file_name: str, layer_origin: str, layer_destiny: str):
        """
        Função para movimentar um arquivo entre duas camadas.

        Parâmetros:
            file_name (str): Nome do arquivo que será movimentado.
            layer_origin (str): Camada de origem do arquivo.
            layer_destiny (str): Camada de destino do arquivo.

        Retorno:
            str: Caminho do arquivo na camada de destino.
        """
        file_name_origin = f'files/{layer_origin}/{file_name}'
        file_destiny = f'files/{layer_destiny}/{file_name}'.replace('.txt', '.csv')
        if os.path.exists(file_name_origin):
            shutil.copy2(src=file_name_origin, dst=file_destiny)
        else:
            raise Exception("Arquivo inexistente: " + file_name_origin)
        logging.info("Os arquivos foram copiados entre as camadas!")
        return file_destiny

    @staticmethod
    def encrypt_data(key_encoded: str, data: str) -> str:
        """
        Função para criptografar uma string.
    
        Parâmetros:
            key_encoded (str): Chave utilizada na criptografia.
            data (str): Valor que será criptografado.
    
        Retorno:
            str: Valor criptografado.
        """
        key = base64.b64decode(key_encoded)
        backend = default_backend()
        cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
        encryptor = cipher.encryptor()
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data.encode()) + padder.finalize()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        ciphertext = base64.b64encode(ciphertext).decode('utf-8')
        return ciphertext
    
    @staticmethod
    def filter_rows_using_values(df: pd.DataFrame, column_name: str, value_filter: str) -> pd.DataFrame:
        """
        Função para filtrar linhas que possuem um determinado valor.

        Parâmetros:
            df (pd.DataFrame): DataFrame onde será filtrado os valores.
            column_name (str): Nome da coluna onde será buscado o valor determinado.
            value_filter (str): Valor que será filtrado na coluna.

        Retorno:
            pd.DataFrame: Dataframe com as linhas contendo o valor filtrado.
        """
        logging.info("Filtrando dados da coluna {0} contendo o valor {1}".format(column_name, value_filter))
        df_returned = df.loc[df[column_name] == value_filter]
        return df_returned

    @staticmethod
    def filter_df_using_column_names(df: pd.DataFrame, columns_names: list) -> pd.DataFrame:
        """
        Função para filtrar os campos de um dataframe.

        Parâmetros:
            df (pd.DataFrame): DataFrame onde será filtrado as colunas.
            columns_names (list): Lista das colunas que serão filtradas no dataframe final.

        Retorno:
            pd.DataFrame: Dataframe com as colunas selecionadas no columns_names.
        """
        logging.info("Filtrando Data Frame com as seguinte(s) coluna(s): {0}".format(columns_names))
        df = df.filter(items=columns_names)
        return df

    @staticmethod
    def join_columns(df: pd.DataFrame, column_1: str, column_2: str, composted_column: str) -> pd.DataFrame:
        """
        Função para criar uma coluna unindo as informações de outras (duas) colunas existentes.

        Parâmetros:
            df (pd.DataFrame): DataFrame onde será criada a coluna composta.
            column_1 (str): Primeira coluna que será utilizada para criar a coluna composta.
            column_2 (str): Segunda coluna que será utilizada para criar a coluna composta.
            composted_column (str): Nome da coluna composta criada.

        Retorno:
            pd.DataFrame: Dataframe com a coluna composta.
        """
        logging.info("Gerando nova coluna a coluna: {0} a partir da união das colunas {1} e {2}"
                     .format(composted_column, column_1, column_2))
        final_value = df[column_1].astype(str) + df[column_2].astype(str)
        df[composted_column] = final_value
        return df

    @staticmethod
    def drop_nulls_from_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Função para retirar registros nulos da coluna informada.

        Parâmetros:
            df (pd.DataFrame): DataFrame onde será buscado o campo null.
            column (str): Coluna onde será buscado o null.

        Retorno:
            pd.DataFrame: Dataframe sem o campo null na coluna especificada.
        """
        logging.info("Removendo a coluna {0} do dataframe.".format(column))
        df = df.loc[~df[column].isna()]
        return df

    @staticmethod
    def add_partition_column(df: pd.DataFrame) -> pd.DataFrame:
        """
        Função para adicionar a coluna de particionamento no DataFrame.

        Parâmetros:
            df (pd.DataFrame): DataFrame onde será adicionado a coluna de particionamento.

        Retorno:
            pd.DataFrame: Dataframe com a coluna de particionamento.
        """
        logging.info("Adicionando a coluna de particionamento ao dataframe.")
        data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["dataIngestao"] = data
        return df

    @staticmethod
    def strip_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Função para realizar strip em campos string.

        Parâmetros:
            df (pd.DataFrame): DataFrame onde será realizado a análise e execução do strip nos campos string.

        Retorno:
            pd.DataFrame: Dataframe com as colunas string sem espaços em excesso.
        """
        logging.info("Removendo os espaços no dataframe")
        df.replace(r'\s*(.*?)\s*', r'\1', regex=True, inplace=True)
        return df

    @staticmethod
    def create_dataframe_from_file(file: str = "", separador: str = ",") -> pd.DataFrame:
        """
        Função para retornar um dataframe a partir de um arquivo CSV existente no ambiente local.

        Parâmetros:
            file (str): Caminho completo para o arquivo csv que será transformado em um dataframe.
            separador (str): Delimitador de colunas utilizado no arquivo para separar as informações, ex: (',', ';').

        Retorno:
            pd.DataFrame: Dataframe contendo as informações do arquivo csv original.
        """
        logging.info("Criando dataframe a partir do arquivo {0} utilizando o separador {1} ".format(file, separador))
        df = pd.read_csv(file, sep=separador)
        logging.info("Dataframe criado")
        return df
    
    
    @staticmethod
    def normalize_file(file_name: str, encoding: str = 'utf-8'):
        """
        Função para normalizar os caracteres de um determinado arquivo.

        Parâmetros:
            file_name (str): Caminho do arquivo onde será realizada a normalização dos caracteres.
            encoding (str): Codificação utilizada para ler o arquivo. Padrão é 'utf-8'.

        Retorno:
            None
        """
        with open(file=file_name, mode="r", encoding=encoding) as f:
            text = ud.normalize('NFD', f.read()).encode('ascii', 'ignore').decode("utf-8")
        with open(file=file_name, mode="w", encoding=encoding) as f:
            f.write(text)
            
            
    @staticmethod
    def trim_file(file: str):
        """
        Função para remover agrupamentos de espaços de um determinado arquivo.

        Parâmetros:
            file (str): Caminho do arquivo onde será realizada a remoção de agrupamentos de espaços.

        Retorno:
            None
        """
        with open(file=file, mode="r", encoding='utf-8') as f:
            text = re.sub(pattern=r'\s{2,}', repl=' ', string=f.read())
        with open(file=file, mode="w", encoding='utf-8') as f:
            f.write(text)
    