import os
import setuptools


DIR = os.path.dirname(os.path.realpath(__file__))


with open(os.path.join(DIR, "README.md"), "r") as f:
    long_description = f.read()


with open(os.path.join(DIR, "requirements.txt"), "r") as f:
    packages = f.read().split("\n")


setuptools.setup(
    name="firestore-collections",
    version="0.5.2",
    author="Lasse Regin Nielsen",
    author_email="lasseregin@gmail.com",
    description="Simple Firestore collection definitions and queries using pydantic schemas and Firestore query API.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/LasseRegin/firestore-collections",
    packages=setuptools.find_packages(),
    install_requires=packages,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    package_data={"": ["LICENSE", "requirements.txt"]},
    include_package_data=True,
)
