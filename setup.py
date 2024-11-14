from setuptools import setup, find_packages

setup(
    name="bitcoin-whale-analysis",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'beautifulsoup4>=4.12.2',
        'pandas>=2.0.0',
        'requests>=2.31.0',
        'selenium>=4.11.2',
        'python-telegram-bot>=20.6',
        'python-dotenv>=1.0.0',
        'lxml',
        'numpy>=1.23.2',
        'chromedriver-autoinstaller>=0.6.0',
        'pyarrow>=14.0.1'
    ],
)