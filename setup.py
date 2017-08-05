from setuptools import setup, find_packages

setup(
    name='data_collecting',
    version='0.1',
    description='Library containing the base for data collectors',
    url='https://github.com/pico-collectors/data_collecting.git',
    license='MIT',
    author='David Fialho',
    author_email='fialho.david@protonmail.com',

    packages=find_packages(),

    install_requires=[],

    extras_require={
        'test': ['pytest', 'hypothesis'],
    },
)
