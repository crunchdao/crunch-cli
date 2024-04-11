#!/usr/bin/env python3

import distutils.sysconfig
import os
import sys

from setuptools import find_packages, setup

package = "crunch"


def find_pth_directory():
    """
    see https://github.com/xolox/python-coloredlogs/blob/65bdfe976ac0bf81e8c0bd9a98242b9d666b2859/setup.py#L64-L84
    """

    if 'bdist_wheel' in sys.argv:
        return "/"

    return os.path.relpath(distutils.sysconfig.get_python_lib(), sys.prefix)


about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, package, '__version__.py')) as f:
    exec(f.read(), about)

with open('requirements/default.txt') as fd:
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
    data_files=[
        (find_pth_directory(), ['crunch-monkeypatch.pth']),
    ],
    python_requires=">=3",
    install_requires=requirements,
    zip_safe=False,
    entry_points={
        'console_scripts': ['crunch=crunch.cli:cli'],
    },
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='package development template'
)
