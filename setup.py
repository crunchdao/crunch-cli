#!/usr/bin/env python3

import os
from setuptools import setup, find_packages

package = "crunch"

about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, package, '__version__.py')) as f:
    exec(f.read(), about)

with open('requirements.txt') as fd:
    requirements = fd.read().splitlines()

with open('README.md') as fd:
    readme = fd.read()

setup(
    name=about['__title__'],
    description=about['__description__'],
    long_description=readme,
    long_description_content_type='text/markdown',
    version=about['__version__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    packages=find_packages(),
    package_data={
        package: [
            "demo-project/*",
            "demo-project/.*",
        ]
    },
    include_package_data=True,
    python_requires=">=3",
    install_requires=requirements,
    zip_safe=False,
    entry_points={
        'console_scripts': ['crunch=crunch.main:cli'],
    },
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='package development template'
)
