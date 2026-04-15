from setuptools import setup, find_packages

setup(
    name="businessCentral-connector",
    version="1.0.0",
    description="Business Central data connector for Microsoft Fabric",
    author="Dream IT Consulting Services",
    
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    
    install_requires=[
        "pandas>=1.5.0",
        "requests>=2.28.0",
        "pyspark>=3.4.0",
        "delta-spark>=2.4.0",
    ],
    
    python_requires=">=3.8",
    include_package_data=True,
)