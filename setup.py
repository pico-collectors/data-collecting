from setuptools import setup, find_packages

setup(
    name='data-collecting',
    version='0.1',
    description='Base library for building data collectors',
    url='https://github.com/pico-collectors/data-collecting',
    license='MIT',
    author='David Fialho',
    author_email='fialho.david@protonmail.com',

    packages=find_packages(),

    install_requires=[],

    extras_require={
        'test': ['pytest'],
    },
)
