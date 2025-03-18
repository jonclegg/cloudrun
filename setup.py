from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="cloudrun",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Run Python code in AWS cloud using Fargate containers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/cloudrun",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.26.0",
        "python-dotenv>=0.19.0",
    ],
    entry_points={
        'console_scripts': [
            'cloudrun=cloudrun.cli:cli',
        ],
    },
) 