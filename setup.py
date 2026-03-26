#!/usr/bin/env python
import codecs
import os
import re

from setuptools import find_packages, setup


def get_version(filename):
    with codecs.open(filename, 'r', 'utf-8') as fp:
        contents = fp.read()
    return re.search(r"__version__ = ['\"]([^'\"]+)['\"]", contents).group(1)


version = get_version(os.path.join('modeldict', '__init__.py'))

with codecs.open('README.rst', 'r', 'utf-8') as readme_file:
    readme = readme_file.read()

with codecs.open('HISTORY.rst', 'r', 'utf-8') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

setup(
    name='django-modeldict-yplan',
    version=version,
    author='DISQUS',
    author_email='opensource@disqus.com',
    maintainer='YPlan',
    maintainer_email='adam@yplanapp.com',
    url='https://github.com/YPlan/django-modeldict',
    description='Stores a model as a dictionary',
    long_description=readme + '\n\n' + history,
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    license='Apache License 2.0',
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Framework :: Django',
        'Framework :: Django :: 4.2',
        'Framework :: Django :: 5.2',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python',
        'Topic :: Software Development'
    ],
)
