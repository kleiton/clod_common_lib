import logging
import glob
import os


class Utils:
    @staticmethod
    def return_files_of_directory(directory=".", regex="*", extension="*"):
        """
        Função para retornar a lista de arquivos em um diretório que correspondem ao padrão fornecido.

        Parâmetros:
            directory (str): Diretório onde os arquivos serão procurados. Padrão é o diretório atual (".").
            regex (str): Padrão para filtrar os arquivos. Padrão é "*".
            extension (str): Extensão dos arquivos a serem filtrados. Padrão é "*".

        Retorno:
            list: Lista de arquivos que correspondem ao padrão fornecido.
        """
        logging.info(f"Retornando a lista de arquivos do diretório: {directory}")

        list_of_files = glob.glob(os.path.join(directory, f"{regex}.{extension}"))
        return list_of_files

    @staticmethod
    def create_ssl_file(env_var: str, local_filename: str):
        """
        Função para criar um arquivo SSL a partir de uma variável de ambiente.

        Parâmetros:
            env_var (str): Nome da variável de ambiente que contém o certificado SSL.
            local_filename (str): Nome do arquivo local onde o certificado será salvo.

        Retorno:
            None
        """
        cert = os.getenv(env_var)
        if cert is None:
            logging.error(f"Variável de ambiente {env_var} não encontrada.")
            return
        
        list_cert = cert.strip().split("|")
        with open(local_filename, "w") as fhandle:
            for line in list_cert:
                fhandle.write(f'{line}\n')
        logging.info(f"Arquivo SSL {local_filename} criado com sucesso a partir da variável de ambiente {env_var}.")