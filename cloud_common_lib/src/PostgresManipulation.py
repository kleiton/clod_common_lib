import logging
import os
import re
import pandas as pd
import sqlalchemy
from sqlalchemy import engine
from sqlalchemy.orm import close_all_sessions


import logging
import os
import re
import sqlalchemy
from sqlalchemy.engine import Engine


class PostgresManipulation:
    engine: Engine = None

    def connect_postgres(self):
        """
        Função para realizar a conexão com a base do Postgresql.

        Parâmetros:
            Sem parâmetros informados, os valores necessários são capturados automaticamente pelas variáveis de ambiente.

        Variáveis de Ambiente:
            db_postgres_user: String
                os.getenv('pg_user')

            db_postgres_password: String
                os.getenv('pg_password')

            db_postgres_host: String
                os.getenv('pg_host')

            db_postgres_port: String
                os.getenv('pg_port')

            db_postgres_database: String
                os.getenv('pg_db')

        Retorno:
            None
        """
        db_postgres_password = os.getenv('DB_POSTGRES_PASSWORD')
        db_postgres_url = os.getenv('DB_POSTGRES_URL')
        db_postgres_user = os.getenv('DB_POSTGRES_USER')

        split_url = re.split(r'[`!@#$%^&*()_+={};:"|,<>?~/]', db_postgres_url)
        url = list(filter(None, split_url))
        db = url[1] + "://" + db_postgres_user + ":" + db_postgres_password + "@" + url[2] + ":" + url[3] + "/" + url[4]

        self.engine = sqlalchemy.create_engine(db, echo=False)
        self.engine.connect()
        logging.info('Conexão com o Postgres realizada com sucesso!')

    def validate_schema(self, schema_name: str = None, table_name: str = None):
        """
        Função para validar se o schema informado é um dos permitidos e se a tabela que se deseja criar
        está no padrão esperado.

        Parâmetros:
            schema_name: String
                Nome do schema

            table_name: String
                Nome da tabela

        Retorno:
            message: void
                Mensagem informando se os parâmetros informados estão dentro do padrão esperado
        """
        schema_type = ['agroinsights', 'agroinsights_relatorios', 'exploratory', 'exploratory-reports']
        table_name_pattern = '^[a-z]+(?:[A-Z][a-z]+)*$'

        if table_name is None:
            if schema_name not in schema_type:
                raise Exception('Os schemas permitidos são: {0}'.format(schema_type))
        else:
            table_patter_match = re.match(table_name_pattern, table_name)

            if schema_name not in schema_type:
                raise Exception('Os schemas permitidos são: {0}'.format(schema_type))
            else:
                self.connect_postgres()
                self.schema_exists(schema_name=schema_name)
            if not table_patter_match:
                raise Exception('O nome da tabela não está no padrão permitido (fonteNome)')
            logging.info('Schema e tabela informados estão dentro do padrão permitido!')
        return True

    def insert_data_into_postgres_from_df(self, dataframe_name: pd.DataFrame, schema_name: str = "",
                                          table_name: str = "", previous_data_action: str = "append"):
        """
        Função para inserir dados no Postgres a partir de um dataframe existente.

        Parâmetros:
            dataframe_name: pd.DataFrame
                DataFrame com os dados que serão inseridos na tabela

            schema_name: String
                Nome do schema onde serão inseridos os dados

            table_name: String
                Nome da tabela onde os dados serão armazenados

            previous_data_action: String
                Ação que irá ser executada quando a carga dos dados for realizada:
                Os tipos permitidos são: 'replace', 'append' para substituir e concatenar respectivamente
                O default é append

        Retorno:
            message: void
                Mensagem informando que os dados foram inseridos
        """
        previous_data_types = ['fail', 'replace', 'append']

        if previous_data_action not in previous_data_types:
            raise Exception('Os tipos de comportamento permitidos são: ' + str(previous_data_types))
        if self.validate_schema(schema_name, table_name):
            df = pd.DataFrame(dataframe_name)
            df.to_sql(str(table_name), con=self.engine, if_exists=previous_data_action, index=False, schema=schema_name)
            logging.info('Os dados do dataframe foram inseridos com sucesso na tabela {0}.{1}'
                         .format(schema_name, table_name))
        return True

    def insert_data_into_postgres_from_csv(self, csv_path: str, schema_name: str, table_name: str, separador: str,
                                           previous_data_action: str = "append"):
        """
        Função para inserir dados no Postgres a partir dos dados existentes em um CSV.

        Parâmetros:
            csv_path: String
                Caminho para o arquivo CSV onde estão os dados que serão inseridos na base

            schema_name: String
                Nome do schema onde a tabela será armazenada

            table_name: String
                Nome da tabela que será criada na base com os dados provenientes do CSV

            separador: String
                Separador utilizado no CSV para identificar novas colunas

            previous_data_action: String
                Ação que irá ser executada quando a carga dos dados for realizada:
                Os tipos permitidos são: 'replace', 'append' para substituir e concatenar respectivamente
                O default é append

        Retorno:
            message: void
                Mensagem informando que os dados foram inseridos na base de dados
        """
        previous_data_types = ['fail', 'replace', 'append']

        if previous_data_action not in previous_data_types:
            raise Exception('Os tipos de comportamento permitidos são: ' + str(previous_data_types))
        if self.validate_schema(schema_name, table_name):
            data = pd.read_csv(csv_path, sep=separador, low_memory=False)
            data.to_sql(table_name, con=self.engine, if_exists=previous_data_action, index=False, schema=schema_name)
            logging.info('Os dados proveninetes do arquivo {0} foram inseridos na tabela {1} com sucesso'
                         .format(csv_path, table_name))

    def create_dataframe_from_table_postgres(self, table_name: str, schema_name: str = 'exploratory'):
        """
        Função para criar um Data Frame a partir dos dados provenientes de uma tabela existente.

        Parâmetros:
            table_name: String
                Nome da tabela existente na base de dados

            schema_name: String
                Nome do schema onde a tabela se encontra

        Retorno:
            df: pd.DataFrame
                Dataframe com os dados existentes na tabela informada
        """
        df = pd.read_sql_table(table_name, schema=schema_name, con=self.engine)
        logging.info('Dataframe criado com os dados provenientes da tabela {0}'.format(table_name))
        return df

    def create_dataframe_from_query_postgres(self, query: str):
        """
        Função para criar um Data Frame a partir de uma query informada.

        Parâmetros:
            query: String
                Query que será realizada na base, os dados retornados irão compor o dataframe criado.
                Necessário especificar na consulta o schema onde a tabela que será consultada se encontra.
                Ex: select * from schema.tabela_teste

        Retorno:
            df: pd.DataFrame
                Dataframe com os dados retornados a partir da query informada
        """
        df = pd.read_sql(query, con=self.engine)
        logging.info('Dataframe criado com sucesso a partir da query informada!')
        return df

    def drop_table_postgres(self, schema_name: str, table_name: str):
        """
        Função para excluir uma tabela existente na base de dados.

        Parâmetros:
            schema_name: String
                Nome do schema onde a tabela será armazenada

            table_name: String
                Nome da tabela que será dropada da base

        Retorno:
            message: void
                Mensagem informando se o processo de Drop foi realizado com sucesso
        """
        insp = self.engine
        if insp.has_table(table_name, schema=schema_name):
            table_name = f'"{table_name}"'
            self.engine.execute(f'DROP TABLE {schema_name}.{table_name}', con=self.engine)
            logging.info('A Tabela {}.{} foi apagada com sucesso!'.format(schema_name, table_name))
        else:
            logging.info('A Tabela {}.{} não existe na base de dados'.format(schema_name, table_name))

    def delete_rows_postgres(self, schema_name: str, table_name: str, field: str, value: str):
        """
        Função para apagar linhas de uma tabela existente na base de dados.

        Parâmetros:
            schema_name: String
                Nome do schema onde a tabela se encontra

            table_name: String
                Nome da tabela existente na base

            field: String
                Campo usado na cláusula WHERE

            value: String
                Valor a ser filtrado na coluna field

        Retorno:
            message: void
                Mensagem informando se o processo de apagar o registro foi executado com sucesso
        """
        insp = self.engine
        if insp.has_table(table_name, schema=schema_name):
            table_name = f'"{table_name}"'
            query = f"""DELETE FROM {schema_name}.{table_name} WHERE "{field}" = '{value}'"""
            self.engine.execute(query, con=self.engine)
            logging.info(f'O Valor {value} foi apagado da tabela {schema_name}.{table_name}!')
        else:
            logging.info(f'A Tabela {schema_name}.{table_name} não existe na base de dados')
            

    def update_rows_postgres(self, schema_name: str, table_name: str, field: str, value: str, condition_column: str,
                            condition_value: str, compare_type: str):
        """
        Função para atualizar linhas de uma tabela existente na base de dados.

        Parâmetros:
            schema_name: String
                Nome do schema onde a tabela se encontra

            table_name: String
                Nome da tabela que será atualizada

            field: String
                Campo usado na cláusula WHERE

            value: String
                Valor a ser filtrado na coluna field

            condition_column: String
                Coluna utilizada como condição para atualização do registro

            condition_value: String
                Valor utilizado na coluna de condição

            compare_type: String
                Forma de comparação, podendo ser = (igual) ou % (iniciando por / finalizando por)

        Retorno:
            message: void
                Mensagem informando se o processo de atualizar o registro foi executado com sucesso
        """
        if compare_type == '=':
            compare = ''
        else:
            compare = '%'

        insp = self.engine
        if insp.has_table(table_name, schema=schema_name):
            self.engine.execute(
                f"UPDATE {schema_name}.{table_name} SET \"{field}\" = '{value}' "
                f"WHERE \"{condition_column}\" {compare_type} '{condition_value}{compare}'")
            logging.info('Os registros foram atualizados com sucesso!')
        else:
            logging.info(f'A Tabela {schema_name}.{table_name} não existe na base de dados')
            
    def disconnect_pg(self):
        """
        Função para encerrar todas as sessões ativas.

        Parâmetros:
            Sem parâmetros

        Retorno:
            message: void
                Mensagem informando que as sessões foram encerradas
        """
        close_all_sessions()
        logging.info('Todas as sessões foram finalizadas com sucesso!')

    def truncate_table_postgre(self, schema_name: str, table_name: str):
        """
        Função para truncar uma tabela existente na base de dados.

        Parâmetros:
            schema_name: String
                Nome do schema onde a tabela se encontra

            table_name: String
                Nome da tabela existente na base

        Retorno:
            message: void
                Mensagem informando se o processo de truncar a tabela foi executado com sucesso
        """
        insp = self.engine
        if insp.has_table(table_name, schema=schema_name):
            schema_name = f'"{schema_name}"'
            table_name = f'"{table_name}"'
            query = f"""TRUNCATE TABLE {schema_name}.{table_name} CASCADE"""
            self.engine.execute(query, con=self.engine)
            logging.info(f'A tabela {schema_name}.{table_name} foi truncada!')
        else:
            logging.info(f'A Tabela {schema_name}.{table_name} não existe na base de dados')

    def grant_permission_postgres(self, schema_name: str):
        """
        Função para conceder permissões de SELECT em todas as tabelas de um schema para o usuário "rouser".

        Parâmetros:
            schema_name: String
                Nome do schema em que as permissões serão concedidas

        Retorno:
            message: void
                Mensagem informando que a permissão foi concedida com sucesso
        """
        schema_name = f'"{schema_name}"'
        self.engine.execute(f'GRANT SELECT  ON ALL TABLES IN SCHEMA {schema_name} TO "rouser"',
                            con=self.engine)
        logging.info('A permissão para o schema {} foi registrada com sucesso!'.format(schema_name))

    def create_schema_postgres(self, schema_name: str):
        """
        Função para criar um novo schema no banco de dados.

        Parâmetros:
            schema_name: String
                Nome do schema que será criado

        Retorno:
            message: void
                Mensagem informando que o schema foi criado com sucesso
        """
        self.engine.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};",
                            con=self.engine)
        logging.info('O schema {} foi criado com sucesso!'.format(schema_name))

    def schema_exists(self, schema_name: str):
        """
        Função para verificar se um schema existe no banco de dados e criá-lo se não existir.

        Parâmetros:
            schema_name: String
                Nome do schema a ser verificado e possivelmente criado

        Retorno:
            message: void
                Mensagem informando se o schema foi criado ou se já existia
        """
        with self.engine.connect() as conn:
            if not conn.dialect.has_schema(conn, schema_name):
                conn.execute(f"CREATE SCHEMA {schema_name}")
                logging.info('O Schema {} foi criado com sucesso!'.format(schema_name))
            else:
                logging.info('O Schema {} já existe na base de dados!'.format(schema_name))