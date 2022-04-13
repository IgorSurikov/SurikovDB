from setuptools import setup, find_packages

setup(
    name='SurikovDB',
    version='1.0.0',
    description='Simple relational database. Use JSONQL',
    author='Igor Surikov',
    author_email='i.surikov@outlook.com',
    url='https://github.com/IgorSurikov/SurikovDB',
    package_dir={'': 'src'},
    packages=find_packages('src', include=['SurikovDB', 'SurikovDB.*']),
    install_requires=[
        'jsonschema>=4.4.0',
        'aiohttp>=3.8.1',
        'platformdirs>=2.5.1'
    ],
    entry_points={
        'console_scripts': ['SurikovDB=SurikovDB.CLISurikovDB:main']
    },
)
