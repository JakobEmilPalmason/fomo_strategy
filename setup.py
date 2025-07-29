from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="fomo-strategy",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A comprehensive data processing pipeline for financial market data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/fomo-strategy",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "split-adjust=scripts.split_adjust_minutes:main",
            "coalesce-data=scripts.coalesce_unadjusted_minutes:main",
            "validate-data=scripts.validation:main",
        ],
    },
) 