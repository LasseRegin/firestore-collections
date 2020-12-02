import setuptools


with open('README.md', 'r') as f:
    long_description = f.read()


with open('./requirements.txt', 'r', encoding='utf-8') as f:
    packages = f.read().split('\n')


setuptools.setup(
    name="firestore-collections",
    version="0.0.2",
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
    python_requires='>=3.6',
)