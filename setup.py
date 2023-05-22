
from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
"google-cloud-bigquery==1.6.1",
"google-cloud-datastore==1.7.3",
"google-cloud-pubsub==0.39.0",
"google-cloud-storage==1.14.0",
"requests==2.31.0",
"pytz==2018.9",
"pytzdata==2019.1",
]

setup(
    name='etl',
    version='1.1.1',
    install_requires=REQUIRED_PACKAGES,
    description='ETL tools for API v2',
    packages = find_packages(),
    package_data = {
        'etl.lib': ["*.json"]
    }
)
