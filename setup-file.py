from setuptools import setup, find_packages

setup(
    name='data_retrieval_script',
    version='0.6',
    packages=find_packages(),
    install_requires=[
        'pyodbc',
        'psutil',
    ],
    entry_points={
        'console_scripts': [
            'data_retrieval=data_retrieval_script:main',
        ],
    },
)
