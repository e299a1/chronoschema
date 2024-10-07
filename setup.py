from setuptools import setup, find_packages
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read()
setup(
    name = 'chronoschema',
    version = '0.0.1',
    author = 'e299a1',
    #author_email = 'john.doe@foo.com',
    license = 'MIT License',
    description = 'Database migration and versioning tool for Microsoft SQL Server.',
    long_description = 'Simple tool to help with database migrations and versioning in Microsoft SQL Server.',
    long_description_content_type = "text/markdown",
    url = 'https://github.com/e299a1/chronoschema',
    py_modules = ['chronoschema'],
    packages = find_packages(),
    install_requires = [requirements],
    python_requires='>=3.12',
    classifiers=[
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    entry_points = '''
        [console_scripts]
        chsc=chronoschema:cli
    '''
)
