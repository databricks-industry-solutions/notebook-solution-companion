from setuptools import find_packages, setup

setup(
    name='solacc',
    version='0.1',
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["databricks-sdk>=0.1.12"],
    license_files = ('LICENSE',)
)
