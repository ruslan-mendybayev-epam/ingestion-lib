from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='ingestion-lib',
    version='0.0.2',
    author='Ruslan Mendybayev',
    author_email='ruslan_mendybayev@epam.com',
    description='Library for data ingestion',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ruslan-mendybayev-epam/ingestion-lib',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    python_requires='>=3.11',
    install_requires=[
        'pydantic==1.10.6',
    ],
)
