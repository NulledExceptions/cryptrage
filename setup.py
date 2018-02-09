from setuptools import find_packages
from setuptools import setup

setup(
    name="cryptrage",
    description="Download ticks from various cryptexchanges and alert on arbitrage",
    author="Giovanni Lanzani",
    install_requires=['pytz', 'tzlocal', 'krakenex', 'gdax', 'BitstampClient',
                      'psycopg2', 'pid', 'PyYAML'],
    extras_require={
        'dev': ['pytest'],
        'test': ['pytest'],
    },
    packages=find_packages()
)
