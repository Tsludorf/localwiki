from setuptools import setup, find_packages

setup(
    name="warlock-ingester",
    version="0.1.0",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'localwiki=src.cli:main',
        ],
    },
    install_requires=[
        'qdrant-client>=1.8.0',
        'click>=8.1.0',
        'requests>=2.31.0',
        'pyyaml>=6.0.0',
        'python-dotenv>=1.0.0',
        'rich>=13.0.0',
        'tika>=1.24',
        'docling>=1.0.0',
    ],
    python_requires='>=3.11',
    author="warlock_ingester Team",
    author_email="team@warlockingester.org",
    description="A local data ingestion system for building knowledge bases using Qdrant vector storage",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/warlock-ingester/warlock-ingester",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)