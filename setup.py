from setuptools import setup

setup(
    name='opp-zone-research',
    version='.1',
    py_modules=['OppZone'],
    include_package_data=True,
    install_requires=[
        'click',
    ],
    entry_points='''
        [console_scripts]
        OppZone=OppZone:cli
    ''',
)