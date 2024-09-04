import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='cloud_common_lib',
    version='1.1.8.6',
    license='MIT',
    description='Repositório com funções úteis para manipulação de dados em pipelines de dados na IBM Cloud.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Kleiton Moraes, Roberto Almeida',
    author_email='kleiton@moraes@icloud.com',
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "ibm-cos-sdk==2.12.0",
        "ibm-cos-sdk-core==2.12.0",
        "ibm-cos-sdk-s3transfer==2.12.0",
        "sqlalchemy==1.4.40",
        "psycopg2==2.9.5",
        "pymongo==4.8.0",
        "pandas>=1.5.0",
        "alchemy-mock==0.4.3",
        "psycopg2-binary==2.9.5",
        "glob2>=0.7",
        "cryptography==43.0.1",
        "numpy==1.23.4"
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Documentation',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ]
)