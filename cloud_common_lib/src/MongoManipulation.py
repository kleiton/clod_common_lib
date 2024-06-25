import json
import logging
import os

import pandas as pd
from pymongo import MongoClient, ASCENDING, HASHED, TEXT, GEOSPHERE, DESCENDING, GEO2D


class MongoManipulation:
    client = None
    database = os.getenv('db_mongo_database')
    uri = ""

    def __init__(self):
        MongoManipulation.create_pem_file()
        client = dict()
        client['db_mongo_ca_cert'] = os.getenv('db_mongo_ca_cert')
        path_cert = (os.getcwd() + '/db_mongo_pem.cert').replace("/", "%2F").replace(" ", "+")
        client['ca_cert'] = path_cert

        db_mongo_user = os.getenv('db_mongo_user')
        db_mongo_password = os.getenv('db_mongo_password')
        db_mongo_host_port = os.getenv('db_mongo_host_port')
        db_mongo_database = os.getenv('db_mongo_database')
        db_mongo_parameters = os.getenv('db_mongo_parameters')
        db_mongo_ca_cert = client['ca_cert']

        self.uri = (
            f"mongodb://{db_mongo_user}:{db_mongo_password}@{db_mongo_host_port}/{db_mongo_database}?authSource={db_mongo_parameters}"
            f"&tls=true&tlsCAFile={db_mongo_ca_cert}&socketTimeoutMS=10800000&connectTimeoutMS=10800000"
        )

    def initiate_connection(self):
        """
        Função para iniciar a conexão com o MongoDB através da URI montada no __init__.

        Parâmetros:
            Sem Parâmetros

        Retorno:
            void: null
        """
        mongo = MongoClient(self.uri)
        self.client = mongo

    def select_database_mongo(self):
        """
        Função para selecionar a database em uso pelo Mongo durante a execução do Script.

        Parâmetros:
            Sem Parâmetros

        Retorno:
            message : void
                Mensagem informando qual database está em uso durante a execução do Script
        """
        self.database = os.getenv('db_mongo_database')
        logging.info("A database em uso é: {0}".format(self.database))

    def insert_data_into_mongo_from_df(self, df, collection: str = '', drop_collection: bool = False):
        """
        Função para inserir dados no mongo a partir de um dataframe existente.

        Parâmetros:
            df (pd.DataFrame): DataFrame com os dados que serão inseridos na collection.
            collection (str): Collection que irá armazenar os dados no MongoDB.
            drop_collection (bool): Indica se a collection deve ser apagada antes da inserção de novos dados.
                                    Valores possíveis: True e False (default).

        Retorno:
            message : void
                Mensagem informando que os dados foram inseridos
        """
        logging.info("Iniciando a inserção dos dados")
        logging.info("Parâmetros informados: Database: {0} / collection: {1} / apagar database antes de inserir {2}"
                     .format(self.database, collection, drop_collection))
        database = self.client[self.database]
        collection_mongo = database[collection]

        df.reset_index(drop=True, inplace=True)
        data_dict = df.to_dict("records")
        if drop_collection:
            collection_mongo.drop()
            logging.info("Collection apagada")
        collection_mongo.insert_many(data_dict)
        logging.info("Dados inseridos na collection {0}".format(collection))
        return True

    @staticmethod
    def create_pem_file():
        """
        Função para criar o arquivo contendo os dados do certificado utilizado para conectar com a base do MongoDB.

        Parâmetros:
            Sem Parâmetros

        Retorno:
            message: void
                Mensagem informando que o certificado foi criado
        """
        db_mongo_pem = os.getenv('db_mongo_pem_cert')
        list_cert = db_mongo_pem.strip().split("|")
        with open("db_mongo_pem.cert", "w") as fhandle:
            for line in list_cert:
                fhandle.write(f'{line}\n')
        logging.info("Certificado de conexão com o MongoDB criado com sucesso!")

    @staticmethod
    def validate_column_index(column, collection_columns, collection):
        """
        Função para validar se a coluna informada existe na collection.

        Parâmetros:
            column (str): Nome da coluna a ser validada.
            collection_columns (list): Lista de colunas disponíveis na collection.
            collection (str): Nome da collection.

        Retorno:
            None
        """
        if column not in collection_columns:
            raise Exception(f'Excessão validate_column_index: Coluna {column} não encontrada na collection {collection}')

    @staticmethod
    def validate_index_type(column, type_of_index):
        """
        Função para validar se o valor do parâmetro de index informado, é um dos permitidos.

        Parâmetros:
            column (str): Nome da coluna.
            type_of_index (str): Tipo de index a ser validado.

        Retorno:
            None
        """
        index_types = [ASCENDING, HASHED, TEXT, GEOSPHERE, DESCENDING, GEO2D]
        if type_of_index not in index_types:
            raise Exception(f'Excessão validate_index_type: Tipo de index {type_of_index} não é válido para a coluna {column}')

    @staticmethod
    def obter_chaves_json(json_obj, prefixo=''):
        """
        Função para ler todas as colunas de um registro no Mongo.

        Parâmetros:
            json_obj (dict): Dicionário contento o registro do Mongo.
            prefixo (str): Prefixo para as chaves (usado para recursão).

        Retorno:
            chaves (list): Lista contendo todos os nomes de colunas, mesmo aninhadas.
        """
        chaves = []
        if isinstance(json_obj, dict):
            for chave, valor in json_obj.items():
                chave_completa = f'{prefixo}.{chave}' if prefixo else chave
                chaves.append(chave_completa)
                chaves.extend(MongoManipulation.obter_chaves_json(valor, chave_completa))
                chaves = list(set(chaves))
        elif isinstance(json_obj, list):
            for i, item in enumerate(json_obj):
                chave_completa = f'{prefixo}' if prefixo else f'[{i}]'
                chaves.extend(MongoManipulation.obter_chaves_json(item, chave_completa))
                chaves = list(set(chaves))
        return chaves

    def create_index(self, collection_name: str = "", indexes=None):
        """
        Função para criar os indices na collection informada.

        Parâmetros:
            collection_name (str): Nome da collection em que os indices serão criados.
            indexes (list): Lista de indices que serão criados, devendo ser inclusos no formato [('CHAVE', Valor)].
                            Onde a chave corresponde ao(s) nomes(s) do(s) campos(s) utilizado(s) no indice e
                            o valor corresponde ao modelo de ordenação desejado, 1 (ASCENDENTE), -1 (DESCENDENTE).

        Retorno:
            message: void
                Mensagem informando que os indices foram criados com sucesso.
        """
        if indexes is None:
            indexes = []
        db = self.client[self.database]
        collection = db[collection_name]
        data = collection.find_one()
        column_names = MongoManipulation.obter_chaves_json(data)
        for index in indexes:
            if isinstance(index, tuple):
                MongoManipulation.validate_column_index(index[0], column_names, collection.name)
            elif isinstance(index, str):
                MongoManipulation.validate_column_index(index, column_names, collection.name)
        for type in indexes:
            if isinstance(type, tuple):
                MongoManipulation.validate_index_type(type[0], type[1])
        if collection_name in db.list_collection_names():
            collection.create_index(indexes)
        logging.info('Índice criado com sucesso!')

    def find_one_mongo(self, collection: str, field: str, value: str):
        """
        Função para retornar um registro associado a collection informada.

        Parâmetros:
            collection (str): Nome da collection em que o valor será pesquisado.
            field (str): Coluna que será utilizada para filtrar os dados.
            value (str): Valor que será pesquisado.

        Retorno:
            document_content (dict): Dados associados ao documento retornado.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        if not field and not value:
            document_content = mycol.find_one()
            logging.info('Os atributos necessários não foram informados corretamente!')
        else:
            document_content = mycol.find_one({field: value})
            logging.info('Foram encontrados valores para os parâmetros informados, retornando um único registro!')
        return document_content

    def find_many_mongo(self, collection: str, field: str, value: str):
        """
        Função para retornar todos os registros associados a collection informada.

        Parâmetros:
            collection (str): Nome da collection em que o valor será pesquisado.
            field (str): Coluna que será utilizada para filtrar os dados.
            value (str): Valor que será pesquisado.

        Retorno:
            document_content (list): Dados associados ao documento retornado.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        data_list = []
        if not field and not value:
            for entry in mycol.find():
                data_list.append(entry)
            logging.info('Os atributos necessários não foram informados corretamente!')
        else:
            for entry in mycol.find({field: value}):
                data_list.append(entry)
            logging.info('Foram encontrados valores para os parâmetros informados, retornando todos os registros!')
        return data_list
    
    def delete_one_mongo(self, collection: str, field: str, value: str):
        """
        Função para apagar um único documento associado a collection informada.

        Parâmetros:
            collection (str): Nome da collection em que o valor será pesquisado.
            field (str): Coluna que será utilizada para filtrar os dados.
            value (str): Valor que será pesquisado.

        Retorno:
            message: void
                Mensagem informando que um registro foi apagado.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        mycol.delete_one({field: value})
        logging.info("Um documento contendo o valor {0} na coluna {1} da base {2} foi apagado com sucesso!"
                    .format(value, field, collection))

    def delete_many_mongo(self, collection: str, field: str, value: str):
        """
        Função para apagar vários documentos associados a collection informada.

        Parâmetros:
            collection (str): Nome da collection em que o valor será pesquisado.
            field (str): Coluna que será utilizada para filtrar os dados.
            value (str): Valor que será pesquisado.

        Retorno:
            message: void
                Mensagem informando que foram apagados registros da base de dados.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        res = mycol.delete_many({field: value})
        logging.info("Foram apagados {0} registros da base de dados".format(res.deleted_count))

    def delete_all_mongo(self, collection: str):
        """
        Função para apagar todos os documentos associados a collection informada.

        Parâmetros:
            collection (str): Nome da collection em que os registros serão apagados.

        Retorno:
            message: void
                Confirmação dos documentos apagados na base.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        res = mycol.delete_many({})
        logging.info("Foram apagados {0} documentos no total".format(res.deleted_count))

    def drop_collection_mongo(self, collection: str):
        """
        Função para apagar a collection informada.

        Parâmetros:
            collection (str): Nome da collection que será apagada da base de dados.

        Retorno:
            message: void
                Confirmação da collection apagada.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        mycol.drop()
        logging.info("A collection {0} foi apagada com sucesso!".format(collection))

    def list_collections_mongo(self):
        """
        Função para listar as collections existentes na base de dados.

        Parâmetros:
            Sem parâmetros

        Retorno:
            collections_list (list): Nomes das collections disponíveis na base.
        """
        mydb = self.client[self.database]
        res = mydb.list_collection_names()
        logging.info('Foram encontradas as seguintes collections: {0}'.format(res))
        return res

    def update_one_mongo(self, collection: str, field: str, old_value: str, new_value: str):
        """
        Função para atualizar dados na collection informada em um único registro.

        Parâmetros:
            collection (str): Nome da collection que será utilizada.
            field (str): Coluna em que o valor será pesquisado.
            old_value (str): Valor atual do registro na coluna pesquisada.
            new_value (str): Novo valor do registro na coluna pesquisada.

        Retorno:
            message: void
                Mensagem informando que o documento foi atualizado.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        mycol.update_one({field: old_value}, {"$set": {field: new_value}})
        logging.info("O documento na collection {0} com o valor antigo {1} foi atualizado para {2}".format(collection, old_value, new_value))

    def update_many_mongo(self, collection: str, field: str, old_value: str, new_value: str):
        """
        Função para atualizar dados na collection informada.

        Parâmetros:
            collection (str): Nome da collection que será utilizada.
            field (str): Coluna em que o valor será pesquisado.
            old_value (str): Valor atual dos registros na coluna pesquisada.
            new_value (str): Novo valor dos registros na coluna pesquisada.

        Retorno:
            message: void
                Mensagem informando que os documentos foram atualizados.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        res = mycol.update_many({field: {"$regex": old_value}}, {"$set": {field: new_value}})
        logging.info("Foram atualizados {0} documentos na collection {1}!".format(res.modified_count, collection))

    def limit_return_mongo(self, collection: str, qtd: int):
        """
        Função para retornar a quantidade de documentos solicitados na collection informada.

        Parâmetros:
            collection (str): Nome da collection em que se deseja retornar um número limitado de registros.
            qtd (int): Quantidade de registros que se deseja retornar.

        Retorno:
            document_content (list): Documentos retornados.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        data_list = []
        for doc in mycol.find().limit(qtd):
            data_list.append(doc)
        logging.info('Foram encontrados os seguintes registros para os critérios informados: {0}'.format(data_list))
        return data_list

    def create_data_for_collection_mongo(self, collection: str, field: str, value: any):
        """
        Função para inserir um novo documento na collection informada.

        Parâmetros:
            collection (str): Nome da collection em que se deseja inserir o registro.
            field (str): Nome da coluna que será criada.
            value (any): Valor que será inserido na coluna criada.

        Retorno:
            message: void
                Mensagem com as informações dos documentos inseridos na collection.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        mydict = {field: value}
        res = mycol.insert_one(mydict)
        logging.info('Foram inseridos os seguintes documentos {0} na collection {1}'.format(res, collection))

    def find_one_max_value_mongo(self, collection: str, field: str):
        """
        Função para encontrar o valor máximo da coluna informada.

        Parâmetros:
            collection (str): Nome da collection que será utilizada.
            field (str): Nome do campo que desejamos retornar o maior valor existente.

        Retorno:
            x (str): Valor máximo encontrado para os parâmetros informados.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        res = mycol.find({}, {field: 1, '_id': 0}).sort(field, -1).limit(1)
        for x in res:
            logging.info('Retornando o valor máximo para a collection {0} e coluna {1}'.format(collection, field))
            return x[field]

    def find_one_min_value_mongo(self, collection: str, field: str):
        """
        Função para encontrar o valor mínimo da coluna informada.

        Parâmetros:
            collection (str): Nome da collection que será utilizada.
            field (str): Nome do campo que desejamos retornar o menor valor existente.

        Retorno:
            x (str): Valor mínimo encontrado para os parâmetros informados.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        res = mycol.find({}, {field: 1, '_id': 0}).sort(field, 1).limit(1)
        for x in res:
            logging.info('Retornando o valor mínimo para a collection {0} e coluna {1}'.format(collection, field))
            return x[field]

    def insert_data_into_mongo_from_csv(self, csv_path, collection: str, sep=',', drop_collection: bool = False):
        """
        Função para inserir novos documentos na collection informada a partir de um csv existente.

        Parâmetros:
            csv_path (str): Caminho para o arquivo .csv onde estão os dados que serão inseridos no mongo.
            collection (str): Nome da collection que se deseja atualizar.
            sep (str): Separador utilizado no arquivo CSV.
            drop_collection (bool): Flag informando se a collection deverá ser apagada antes de inserir os dados.

        Retorno:
            message: void
                Mensagem com as informações dos documentos inseridos na collection.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        if drop_collection:
            mycol.drop()
            logging.info("A collection {0} foi apagada com sucesso!".format(collection))
        data = pd.read_csv(csv_path, sep=sep, encoding='utf-8')
        payload = json.loads(data.to_json(orient='records'))
        mycol.insert_many(payload)
        logging.info('Os dados do arquivo {0} foram inseridos na collection {1}'.format(csv_path, collection))
        return True

    def disconnect(self):
        """
        Função para fechar a conexão com o MongoDB.

        Parâmetros:
            Sem Parâmetros

        Retorno:
            void: null
        """
        self.client.close()
        
    def find_many_mongo_select_columns(self, collection: str, columns: dict):
        """
        Função para retornar todos os registros associados a collection informada limitando as colunas.

        Parâmetros:
            collection (str): Nome da collection em que o valor será pesquisado.
            columns (dict): Dicionário onde as colunas que não devem ser consultadas têm valor igual a 0.

        Retorno:
            document_content (list): Dados associados ao documento retornado.
        """
        mydb = self.client[self.database]
        mycol = mydb[collection]
        data_list = []
        data = mycol.find({}, columns)
        for entry in data:
            data_list.append(entry)
        return data_list