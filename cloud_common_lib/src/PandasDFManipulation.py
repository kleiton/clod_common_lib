import logging
import pandas as pd
from datetime import datetime
from typing import List


class PandasDFManipulation:

    @staticmethod
    def filter_rows_using_values(df: pd.DataFrame, column_name: str, value_filter: str):
        """
        ****************************************************************************************************************
        Função para filtrar linhas que possuem um determinado valor
        ****************************************************************************************************************

        ----------------------------------------------------------------------------------------------------------------
        Parâmetros
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            DataFrame onde será filtrado os valores

        column_name: String
            Nome da coluna onde será buscado o valor determinado

        value_filter: String
            Valor que será filtrado na coluna

        ----------------------------------------------------------------------------------------------------------------
        Retorno
        ----------------------------------------------------------------------------------------------------------------

        df : pd.Dataframe
            Dataframe com as linhas contendo o valor filtrado
        """
        logging.info("Filtrando dados da coluna {0} contendo o valor {1}".format(column_name, value_filter))

        df_returned = df.loc[df[column_name] == value_filter]
        return df_returned

    @staticmethod
    def filter_df_using_column_names(df: pd.DataFrame, columns_names: List[str]):
        """
        Função para filtrar os campos de um dataframe.

        Parâmetros:
            df: pd.DataFrame
                DataFrame onde será filtrado as colunas

            columns_names: List[str]
                Lista das colunas que serão filtrados no dataframe final

        Retorno:
            df: pd.Dataframe
                Dataframe com as colunas selecionadas no columns_names
        """
        logging.info("Filtrando Data Frame com as seguinte(s) coluna(s): {0}".format(columns_names))

        df = df.filter(items=columns_names)
        return df

    @staticmethod
    def join_columns(df, column_1: str, column_2: str, composted_column: str):
        """
        ****************************************************************************************************************
        Essa função irá criar uma coluna unindo as informações de outras (duas) colunas existentes
        ****************************************************************************************************************

        ----------------------------------------------------------------------------------------------------------------
        Parâmetros
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            DataFrame onde será criada a coluna composta

        column_1: String
            Primeira coluna que será utilizada para criar a coluna composta

        column_2: String
            Segunda coluna que será utilizada para criar a coluna composta

        composted_column: String
            Nome da coluna composta criada

        ----------------------------------------------------------------------------------------------------------------
        Retorno
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            Dataframe com a coluna composta
        """
        logging.info("Gerando nova coluna a coluna: {0} a partir da união das colunas {1} e {2}"
                     .format(composted_column, column_1, column_2))

        final_value = df[column_1].astype(str) + df[column_2].astype(str)
        df.loc[:, composted_column] = final_value
        return df

    @staticmethod
    def drop_nulls_from_column(df, column: str):
        """
        ****************************************************************************************************************
        Função para retirar registros nulos da coluna informada
        ****************************************************************************************************************

        ----------------------------------------------------------------------------------------------------------------
        Parâmetros
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            DataFrame onde será buscado o campo null

        column: String
            Coluna onde será buscado o null

        ----------------------------------------------------------------------------------------------------------------
        Retorno
        ----------------------------------------------------------------------------------------------------------------

        df: pd.Dataframe
            Dataframe sem o campo null na coluna especificada
        """
        logging.info("Removendo a coluna {0} do dataframe.".format(column))

        df = df.loc[~df[column].isna()]
        return df

    @staticmethod
    def add_partition_column(df: pd.DataFrame):
        """
        ****************************************************************************************************************
        Função para adicionar a coluna de particionamento no DataFrame
        ****************************************************************************************************************

        ----------------------------------------------------------------------------------------------------------------
        Parâmetros
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            DataFrame onde será adicionado a coluna de particionamento

        ----------------------------------------------------------------------------------------------------------------
        Retorno
        ----------------------------------------------------------------------------------------------------------------

        df : pd.Dataframe
            Dataframe com a coluna de particionamento
        """

        logging.info("Adicionando a coluna de particionamento ao dataframe.")

        data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.loc[:, "dataIngestao"] = data
        return df

    @staticmethod
    def strip_df(df: pd.DataFrame):
        """
        ****************************************************************************************************************
        Função para realizar strip em campos string
        ****************************************************************************************************************

        ----------------------------------------------------------------------------------------------------------------
        Parâmetros
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            DataFrame onde será realizado a análise e execução do strip nos campos string

        ----------------------------------------------------------------------------------------------------------------
        Retorno
        ----------------------------------------------------------------------------------------------------------------

        df: pd.Dataframe
            Dataframe com as colunas string sem espaços em excesso
        """
        logging.info("Removendo os espaços no dataframe")

        df.replace(r'\s*(.*?)\s*', r'\1', regex=True, inplace=True)
        return df

    @staticmethod
    def create_dataframe_from_file(file: str = "", separador: str = ""):
        """
        ****************************************************************************************************************
        Função para retornar um dataframe a partir de um arquivo CSV existente no ambiente local
        ****************************************************************************************************************

        ----------------------------------------------------------------------------------------------------------------
        Parâmetros
        ----------------------------------------------------------------------------------------------------------------

        file: String
            Caminho completo para o arquivo csv que será transformado em um dataframe

        separador: String
            Delimitador de colunas utilizado no arquivo para separar as informações, ex: (',', ';')

        ----------------------------------------------------------------------------------------------------------------
        Retorno
        ----------------------------------------------------------------------------------------------------------------

        df: pd.DataFrame
            Dataframe contendo as informações do arquivo csv original
        """
        logging.info("Criando dataframe a partir do arquivo {0} utilizando o separador {1} ".format(file, separador))

        df = pd.read_csv(file, sep=separador)

        logging.info("Dataframe criado")
        return df
