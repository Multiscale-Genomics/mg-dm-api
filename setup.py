from setuptools import setup, find_packages

setup(
    name='dmp',
    version='0.0.1',
    description='MuG DMP API',
    
    url='http://www.multiscalegenomics.eu',
    download_url='https://github.com/Multiscale-Genomics/mg-dm-api',
    
    author='Mark McDowall',
    author_email='mcdowall@ebi.ac.uk',
    
    license='Apache 2.0',
    
    packages=find_packages(),
    
    install_requires = [
        'pymongo>=3.3', 'mongomock>=3.7'
    ]
)
