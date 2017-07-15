import os
from setuptools import find_packages

version = '1.0.0'

def resolve_requirements():
    requirements = []
    with open('requirements.txt') as f:
        line = f.readline().strip()
        if not line.startswith("#"):
            requirements.append(line.strip("\n"))

setup_args = dict(
    name='pymr',
    version=version,
    packages=find_packages(exclude=["*.test", "*.test.*", "test.*", "test", "script", "private"]),
    install_requires=resolve_requirements(),
    include_package_data=True,
    # scripts=["scripts/quantlib"],
    url='',
    license='GNU',
    author='SnowWalkerJ',
    author_email='jike3212001@163.com',
    description='',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: GNU License',

        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',

        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
else:
    setup_args['install_requires'] = [
        'pyinstaller',
    ]

setup(**setup_args)
