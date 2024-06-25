# cloud_common_lib

Repositório com funções úteis para manipulação de dados na IBM Cloud Object Store, tratamento de logs, e manipulação de dados em MongoDB, PostgreSQL e arquivos locais. Inclui operações de leitura, escrita, atualização e consulta para facilitar o gerenciamento de dados.

## Funcionalidades

- **Manipulação de dados na IBM Cloud utilizando o COS (Cloud Object Store)**: Funções para carregar, baixar e gerenciar dados armazenados na nuvem da IBM.
- **Tratamento de logs**: Ferramentas para processamento e análise de arquivos de log.
- **Manipulação de dados no MongoDB**: Funções para inserir, atualizar, deletar e consultar documentos em bancos de dados MongoDB.
- **Manipulação de dados no PostgreSQL**: Ferramentas para executar operações em bancos de dados PostgreSQL, incluindo consultas e atualizações.
- **Manipulação de arquivos**: Funções para ler, escrever e manipular arquivos locais.
- **Manipulação de DataFrames com Pandas**: Funções para manipular dados em DataFrames do Pandas.
- **Métodos de Sanitização**: Ferramentas para sanitização e limpeza de dados.
- **Utilidades Gerais**: Funções auxiliares para diversas operações.

## Estrutura do Repositório

```plaintext
├── cloud_common_lib.log
├── cloud_common_lib
│   ├── COSManipulation.py
│   ├── FileManipulation.py
│   ├── Logging.py
│   ├── MongoManipulation.py
│   ├── PandasDFManipulation.py
│   ├── PostgresManipulation.py
│   ├── SanitizationMethods.py
│   ├── Utils.py
│   ├── __init__.py
│   ├── db_mongo_pem.cert
│   ├── lib_teste.py
│   └── src.log
└── files
    ├── exploratory
    │   └── dados.txt
    ├── features
    │   └── dados.txt
    ├── raw
    │   └── dados.txt
    ├── refined
    │   └── dados.txt
    ├── sandbox
    │   └── dados.txt
    └── trusted
        └── dados.txt
```

### Camadas de Dados

Para manter o histórico do tratamento das informações, os dados são organizados em diferentes camadas na pasta `files`:

- **Exploratory**: Contém dados exploratórios utilizados para análises iniciais e investigações ad-hoc. Esses dados são usados para entender a natureza dos dados brutos e para experimentações rápidas.
  
- **Features**: Contém dados transformados em variáveis que podem ser usadas em modelos de machine learning ou outras análises avançadas. São derivados dos dados raw e refined após processamento e feature engineering.
  
- **Raw**: Contém dados brutos que chegam diretamente das fontes, sem qualquer processamento. Essa camada é usada para armazenar a versão original dos dados antes de qualquer transformação.
  
- **Refined**: Contém dados processados e transformados para análises mais profundas e modelagem. Esses dados passaram por um nível básico de limpeza e transformação para torná-los utilizáveis para análises padrão.
  
- **Sandbox**: Contém dados utilizados para testes e experimentações que não são parte do fluxo de produção. Essa camada é útil para desenvolvimento e validação de novos métodos e processos.
  
- **Trusted**: Contém dados confiáveis e validados, prontos para uso em aplicações críticas e relatórios finais. Esses dados passaram por validações rigorosas e são considerados de alta qualidade para uso em produção.

## Como Usar

1. Clone o repositório:
    ```bash
    git clone https://github.com/kleiton/clod_common_lib.git
    cd cloud_common_lib
    ```

2. Instale as dependências:
    - Certifique-se de ter o Python instalado.
    - Instale as bibliotecas necessárias:
    ```bash
    pip install -r requirements.txt
    ```

3. Importe e use as funções conforme necessário:
    ```python
    import logging
    from cloud_common_lib.COSManipulation import COSManipulation
    from cloud_common_lib.Logging import Logging
    from cloud_common_lib.MongoManipulation import MongoManipulation
    from cloud_common_lib.FileManipulation import FileManipulation
    from cloud_common_lib.PandasDFManipulation import PandasDFManipulation
    from cloud_common_lib.PostgresManipulation import PostgresManipulation
    from cloud_common_lib.Utils import Utils
    from cloud_common_lib.SanitizationMethods import SanitizationMethods
    ```


## Contribuição

Contribuições são bem-vindas! Por favor, abra uma issue ou envie um pull request para melhorias.

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.