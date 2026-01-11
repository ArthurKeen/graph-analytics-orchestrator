"""
Setup script for Graph Analytics AI library.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="graph-analytics-orchestrator",
    version="1.2.0",
    description="Common library for orchestrating ArangoDB Graph Analytics Engine (GAE) operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Arthur Keen",
    author_email="",
    url="https://github.com/ArthurKeen/graph-analytics-orchestrator",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "python-arango>=7.0.0",
        "requests>=2.28.0",
        "python-dotenv>=0.19.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.991",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
    ],
    keywords="arangodb graph analytics gae orchestration",
)

