from setuptools import setup, find_packages

setup(
    name='ExtractPDFDataTools',  # Choose a name for your project
    version='0.1',  # Define a version number
    packages=find_packages(),  # Automatically discover all packages and subpackages
    install_requires=[
        'pyspark',  # As you're using PySpark, include it as a dependency
        # Add other dependencies here
        # 'numpy',
        # 'pandas',
        # etc.
    ],

    # If there are data files included in your packages that need to be installed, specify them here
    # If using Python 2.6 or earlier, these have to be included in MANIFEST.in as well.
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        # '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        # 'hello': ['*.msg'],
    },

    # Although 'package_dir' is hardly ever needed, here's how you'd use it:
    # to tell setuptools to consider the 'src' directory the root of your
    # source packages, you'd use something like this:
    # package_dir={'': 'src'},

    # Specify your project's Python version support (adjust as necessary)
    python_requires='>=3.6',

    # Metadata to display on PyPI
    author='Hanbo Wang',  # Fill in your name
    author_email='harrywong2017@gmail.com',  # Fill in your email
    description='A project for extracting data from PDFs using PySpark',  # Provide a short description
    keywords='pyspark pdf data-extraction',  # Keywords for your project
    url='https://github.com/mag1cfrog/LDOE_Projects/tree/master',  # Link to your project's repository
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Choose a license
        'Programming Language :: Python :: 3',  # Specify the Python versions your project supports
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
