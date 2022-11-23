
from setuptools import find_namespace_packages, setup

setup(
    name="dbt-infer",
    version="1.2.0",
    description="The Infer adapter plugin for dbt",
    long_description="The Infer adapter plugin for dbt",
    author="Infer",
    author_email="support@inferlabs.io",
    url="https://github.com/inferlabs/dbt-infer",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core>=1.2.0.",
        "requests"
    ],
)
