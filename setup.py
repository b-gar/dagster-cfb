from setuptools import find_packages, setup

setup(
    name="dagster_project",
    packages=find_packages(exclude=["dagster_project_tests"]),
    install_requires=[
        "dagster",
        "pandas",
        "requests",
        "google-cloud-bigquery",
        "db-dtypes"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
