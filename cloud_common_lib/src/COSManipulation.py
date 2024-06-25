import io
import logging
import os
from operator import attrgetter
from pathlib import Path

import ibm_boto3
import pandas as pd
from ibm_botocore.config import Config
from ibm_botocore.exceptions import ClientError

CLIENT_ERROR_ = "CLIENT ERROR: {0}\n"

class COSManipulation:

    def __init__(self):
        # Esses parâmetros serão obtidos do Secret Manager do Code Engine
        cos_api_key_id = os.environ.get('cos_api_key_id') 
        cos_endpoint = os.environ.get('cos_endpoint')
        cos_resource_crn = os.environ.get('cos_resource_crn')
        cos_auth_endpoint = os.getenv('cos_auth_endpoint')

        self._cos = ibm_boto3.resource(
            service_name='s3',
            ibm_api_key_id=cos_api_key_id,
            ibm_service_instance_id=cos_resource_crn,
            ibm_auth_endpoint=cos_auth_endpoint,
            config=Config(signature_version='oauth'),
            endpoint_url=cos_endpoint
        )

        self._cos_client = ibm_boto3.client(
            service_name='s3',
            ibm_api_key_id=cos_api_key_id,
            ibm_service_instance_id=cos_resource_crn,
            ibm_auth_endpoint=cos_auth_endpoint,
            config=Config(signature_version='oauth'),
            endpoint_url=cos_endpoint
        )

    def get_buckets(self):
        """
        Função para retornar a lista buckets disponíveis no ambiente.

        Parâmetros:
            Nenhum

        Retorno:
            buckets_list (List): Lista com os nomes dos buckets disponíveis no ambiente.
        """
        logging.info("Retornando a lista de buckets")
        buckets_list = []
        try:
            buckets = self._cos.buckets.all()
            for bucket in buckets:
                buckets_list.append(bucket.name)
                logging.info("Nome do bucket: {0}".format(bucket.name))
            return buckets_list
        except ClientError as be:
            logging.error(CLIENT_ERROR_.format(be))
        except Exception as e:
            logging.error("Não foi possível retornar a lista de buckets: {0}".format(e))

    def get_bucket_contents_cos(self, folder=""):
        """
        Função para retornar a lista de arquivos dentro do bucket/pasta informado.

        Parâmetros:
            folder (str): Nome da pasta onde estão os arquivos (ex: raw/, exploratory/, features/, refined/, sandbox/, trusted/).
                          Se não for informada a pasta desejada, será retornado o conteúdo a partir do diretório raiz.

        Retorno:
            files_list (List): Lista com os nomes dos arquivos disponíveis no bucket.
        """
        logging.info("Retornando a lista de conteúdos do bucket : {0}".format(os.environ.get('cos_bucket_name')))
        try:
            if not folder:
                files = self._cos.Bucket(os.environ.get('cos_bucket_name')).objects.all()
            else:
                files = self._cos.Bucket(os.environ.get('cos_bucket_name')).objects.filter(Delimiter='/', Prefix=folder)
            files_list = [file.key for file in files]
            return files_list
        except ClientError as be:
            logging.error(CLIENT_ERROR_.format(be))
            logging.error("Não foi possível retornar o conteúdo do bucket: {0}".format(be))
        except Exception as e:
            logging.error("Não foi possível retornar o conteúdo do bucket: {0}".format(e))

    def create_text_file_cos(self, file_name="nome_arquivo", remote_path="", file_content=""):
        """
        Função para criar um arquivo dentro do bucket informado.

        Parâmetros:
            file_name (str): Nome do arquivo que será criado.
            remote_path (str): Caminho onde o arquivo será criado no COS.
            file_content (str): Conteúdo do arquivo, se nenhum valor for informado, o arquivo ficará vazio.

        Retorno:
            bool: True se o arquivo foi criado com sucesso, False caso contrário.
        """
        logging.info("Criando um novo arquivo: {0}".format(file_name))
        try:
            self._cos.Object(os.environ.get('cos_bucket_name'), remote_path + file_name).put(Body=file_content)
            logging.info("Item: {0} criado!".format(file_name))
            return True
        except ClientError as be:
            logging.error(CLIENT_ERROR_.format(be))
            return False
        except Exception as e:
            logging.error("Não foi possível criar o arquivo: {0}".format(e))
            return False

    def delete_file_cos(self, file_name=""):
        """
        Função para apagar arquivos disponíveis no COS.

        Parâmetros:
            file_name (str): Caminho onde o arquivo será apagado no COS.

        Retorno:
            bool: True se o arquivo foi apagado com sucesso, False caso contrário.
        """
        logging.info("Apagando arquivo: {0}".format(file_name))
        try:
            self._cos.Object(os.environ.get('cos_bucket_name'), file_name).delete()
            logging.info("Item: {0} apagado com sucesso!".format(file_name))
            return True
        except ClientError as be:
            logging.error(CLIENT_ERROR_.format(be))
            return False
        except Exception as e:
            logging.error("Não foi possível apagar o arquivo: {0}".format(e))
            return False

    def upload_file_cos(self, local_file_path="", remote_file_path=""):
        """
        Função para fazer upload de arquivos para a Cloud Object Storage (COS) da IBM.

        Parâmetros:
            local_file_path (str): Caminho do arquivo local que será enviado.
            remote_file_path (str): Caminho do diretório remoto onde será armazenado o arquivo enviado.

        Retorno:
            bool: True se o upload foi realizado com sucesso, False caso contrário.
        """
        logging.info("Fazendo upload do arquivo " + local_file_path + " para o COS...")
        try:
            self._cos_client.upload_file(Filename=local_file_path, Bucket=os.environ.get('cos_bucket_name'), Key=remote_file_path)
            logging.info("Arquivo {0} enviado com sucesso para o diretório {1}!".format(local_file_path, remote_file_path))
            return True
        except Exception as e:
            logging.error("Erro ao fazer upload: {0}".format(e))
            return False

    def return_file_content_cos(self, filename=None):
        """
        Função para retornar o conteúdo de determinado arquivo disponível no bucket/pasta informado.

        Parâmetros:
            filename (str): Nome do arquivo. Caso o arquivo esteja dentro de uma estrutura de pasta, informar o caminho completo.

        Retorno:
            str: Conteúdo do arquivo solicitado.
        """
        logging.info("Retornando o conteúdo do bucket: {0}, para o item: {1}".format(os.environ.get('cos_bucket_name'), filename))
        try:
            file_cos = self._cos.Object(os.environ.get('cos_bucket_name'), filename).get()
            content = file_cos["Body"].read()
            logging.info(content)
            return content
        except ClientError as be:
            logging.error(CLIENT_ERROR_.format(be))
        except Exception as e:
            logging.error("Não foi possível retornar o conteúdo do item solicitado: {0}".format(e))

    def create_dataframe_from_file_on_cos(self, filename: str = ""):
        """
        Função para retornar o conteúdo de determinado arquivo em formato de dataframe.

        Parâmetros:
            filename (str): Nome do arquivo. Caso o arquivo esteja dentro de uma estrutura de pasta, informar o caminho completo.

        Retorno:
            pd.DataFrame: DataFrame com os dados do arquivo do COS.
        """
        logging.info("Criando dataframe a partir do arquivo {0}".format(filename))
        try:
            bucket_object = self._cos.Object(os.environ.get('cos_bucket_name'), filename)
            csv_file = bucket_object.get()
            file_data = io.StringIO(csv_file["Body"].read().decode('utf-8'))
            df = pd.read_csv(file_data)
            logging.info("Dataframe criado")
            return df
        except ClientError as be:
            logging.error(CLIENT_ERROR_.format(be))
        except Exception as e:
            logging.error("Não foi possível retornar o conteúdo do item solicitado: {0}".format(e))

    def download_file_cos(self, file_name=''):
        """
        Função para baixar o arquivo do COS.

        Parâmetros:
            file_name (str): Caminho do arquivo no COS. Caso o arquivo esteja dentro de uma estrutura de pasta, informar o caminho completo.

        Retorno:
            None
        """
        cwd = os.getcwd() + '/files/'
        path = file_name.split("/")[0:-1]
        path_without_filename = cwd + '/'.join(path)
        if not os.path.exists(path_without_filename):
            os.makedirs(path_without_filename)
        with open(cwd + file_name, 'wb') as data:
            self._cos_client.download_fileobj(os.environ.get('cos_bucket_name'), file_name, data)
            
    def download_directory_cos(self, dir_name=''):
        """
        Função para baixar pastas completas do COS.

        Parâmetros:
            dir_name (str): Nome da pasta para realizar o download. Deve terminar com '/'.

        Retorno:
            None
        """
        assert dir_name.endswith('/')
        bucket = self._cos.Bucket(os.environ.get('cos_bucket_name'))
        objs = bucket.objects.filter(Prefix=dir_name)
        sorted_objs = sorted(objs, key=attrgetter("key"))
        for obj in sorted_objs:
            cwd = os.getcwd()
            path = Path(os.path.dirname(cwd + '/' + dir_name + obj.key))
            path.mkdir(parents=True, exist_ok=True)
            if not obj.key.endswith("/"):
                bucket.download_file(obj.key, str(path) + "/" + os.path.split(obj.key)[1])

    def trigger_cos(self, files, trigger_cos_path: str, trigger_local_path: str):
        """
        Função para disparar alguma ação baseada em um trigger.

        Parâmetros:
            files (List[str]): Lista de arquivos para verificar no COS.
            trigger_cos_path (str): Caminho no COS onde está o arquivo a ser monitorado.
            trigger_local_path (str): Caminho local onde está o arquivo que irá disparar a ação.

        Retorno:
            None
        """
        cos_files = self.get_bucket_contents_cos()
        files_in_cos_list = [file for file in files if file in cos_files]

        if files == files_in_cos_list:
            with open(trigger_local_path, 'w') as job_trigger:
                job_trigger.write(".")

            self.upload_file_cos(trigger_local_path, trigger_cos_path)
            logging.info("Trigger disparado!")
        else:
            file_missing = [file for file in files if file not in files_in_cos_list]
            for file in file_missing:
                logging.info('Arquivo Não encontrado: ' + str(file))

    def upload_file_from_folder_cos(self, files: list, cos_directory: str):
        """
        Função para fazer upload de arquivos para a Cloud Object Storage (COS) da IBM.

        Parâmetros:
            files (List[str]): Lista de caminhos dos arquivos locais que serão enviados.
            cos_directory (str): Caminho do diretório remoto onde serão armazenados os arquivos enviados.

        Retorno:
            None
        """
        if files is None:
            files = []

        try:
            for file in files:
                logging.info("Escrevendo arquivo {0} no diretório {1}".format(os.path.basename(file), cos_directory))

                self.upload_file_cos(local_file_path=file, remote_file_path=cos_directory + os.path.basename(file))

                logging.info("Arquivo {0} enviado com sucesso para o diretório {1}!".format(os.path.basename(file), cos_directory))
        except Exception as e:
            logging.error("Erro ao fazer upload: {0}".format(e))  