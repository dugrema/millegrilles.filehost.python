from setuptools import setup, find_packages
from os import environ

__VERSION__ = environ.get('VBUILD') or '2024.8.0'


setup(
    name='millegrilles_filehost_python',
    version=__VERSION__,
    packages=find_packages(),
    url='https://github.com/dugrema/millegrilles.filehost.python',
    license='AFFERO',
    author='Mathieu Dugre',
    author_email='mathieu.dugre@mdugre.info',
    description='Base web pour les applictions MilleGrilles avec client',
    install_requires=[
        'pytz>=2020.4',
        'aiohttp>=3.8.1,<4',
        'requests>=2.28.1,<3',
        'pyjwt',
        'aiohttp-session==2.12.0'
    ]
)
