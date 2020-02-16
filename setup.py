'''
@author: Arkadiusz Dzięgiel <arkadiusz.dziegiel@glorpen.pl>
'''

import os
import re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

with open('%s/src/glorpen/deduplication/__init__.py' % (here,), 'rt') as f:
    data = f.read()
    version = re.search(r'^__version__\s*=\s*"([^"]+)', data, re.MULTILINE).group(1)
    description = re.search(r'^__description__\s*=\s*"([^"]+)', data, re.MULTILINE).group(1)

requires = [
    'tqdm~=4.42',
    'ioctl_opt~=1.2'
]

suggested_require = []
dev_require = []
tests_require = ['unittest']

setup(name='glorpen.deduplication',
      version=version,
      description=description,
      long_description=README,
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Programming Language :: Python :: 3 :: Only',
          'Operating System :: POSIX',
          'Topic :: System :: Archiving :: Backup',
          'Topic :: System :: Filesystems',
          'Topic :: Utilities',
      ],
      author='Arkadiusz Dzięgiel',
      author_email='arkadiusz.dziegiel@glorpen.pl',
      url='https://github.com/glorpen/glorpen-deduplication',
      keywords='filesystem files deduplication',
      packages=["glorpen.%s" % i for i in find_packages('src/glorpen', exclude=['deduplication.tests'])],
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=True,
      extras_require={
          'testing': tests_require + suggested_require,
          'development': dev_require + tests_require + suggested_require,
          'suggested': suggested_require
      },
      install_requires=requires,
      entry_points = {
        'console_scripts': [
            'glorpen-deduplication = glorpen.deduplication.cli:execute',
        ]
      },
      test_suite='glorpen.deduplication.tests',
)