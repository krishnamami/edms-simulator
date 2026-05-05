from setuptools import setup, find_packages

setup(
    name="edms-simulator",
    version="0.1.0",
    description="Enterprise Data Management System simulator (production Aurora + Redis + S3 + ECS).",
    author="EDMS Team",
    packages=find_packages(exclude=("tests", "tests.*", "scripts", "infra")),
    python_requires=">=3.10",
    include_package_data=True,
)
