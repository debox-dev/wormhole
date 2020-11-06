import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "wormhole",
    version = "0.0.1",
    author = "Lital Natan",
    author_email = "litaln@gmail.com",
    description = ("Minimal RPC and message distribution framework"),
    license = "MIT",
    install_requires = ["redis>=3.3.11"],
    keywords = "",
    packages=find_packages(),
    long_description="Please see README.md",
    url="https://github.com/debox-dev/wormhole",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
