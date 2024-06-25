import logging
import os
import sys

class Logging:
    
    @staticmethod
    def initiate_log(log_file_mode='a', log_handle_mode='y'):
        """
        Função para iniciar o registro de log no projeto.

        Parâmetros:
            log_file_mode (str): Modo de registro do log no arquivo criado. Se definido como 'a' (default), os logs serão apendados no arquivo.
                                 Se definido como 'w', os logs irão sobrescrever o arquivo da execução anterior.
            log_handle_mode (str): Modo de registro do log. Se definido como 'y' (default), os logs serão registrados no arquivo e no console.
                                   Se definido como 'n', os logs serão registrados apenas no arquivo.

        Retorno:
            None
        """
        cwd = os.getcwd()
        path = os.path.dirname(cwd)
        projeto = os.path.basename(path)
        log_filename = os.path.join(cwd, f"{projeto}.log")

        if log_handle_mode == 'y':
            handle_mode = [logging.FileHandler(log_filename, mode=log_file_mode),
                           logging.StreamHandler(sys.stdout)]
        else:
            handle_mode = [logging.FileHandler(log_filename, mode=log_file_mode)]

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                            datefmt='%d-%m-%Y %H:%M:%S',
                            handlers=handle_mode)
        logging.info('Registro de logs iniciados no projeto!')