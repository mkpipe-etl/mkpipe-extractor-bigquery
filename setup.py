from setuptools import setup, find_packages

setup(
    name='mkpipe-extractor-bigquery',
    version='0.1.0',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe'],
    include_package_data=True,
    entry_points={
        'mkpipe.extractors': [
            'bigquery = mkpipe_extractor_bigquery:BigQueryExtractor',
        ],
    },
    description='Google BigQuery extractor for mkpipe.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
