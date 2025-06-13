import setuptools

# Read the contents of your README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="edidio_control_py",
    version="0.1.1",
    author="Michael Howes",
    author_email="michael@creativelighting.com.au",
    description="Python library for controlling the Control Freak eDIDIO S10 device.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CreativeLightingAdmin/edidio_control_py",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Home Automation",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: AsyncIO",
    ],
    python_requires=">=3.9",  
    install_requires=[
        "protobuf>=3.0"
    ],
)
