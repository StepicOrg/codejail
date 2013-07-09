from setuptools import setup

# Dynamically calculate the version based on src.VERSION.
version = __import__('codejail').get_version()

setup(
    name="codejail",
    version=version,
    packages=['codejail'],
)
