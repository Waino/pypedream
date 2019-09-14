#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup

import re
main_py = open('pypedream/__init__.py').read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", main_py))

#requires = [ ]

setup(name='pypedream',
      version=metadata['version'],
      author=metadata['author'],
      author_email='stig-arne.gronroos@aalto.fi',
      #url='',
      description='pypedream - Utility library for scriptwriting',
      packages=['pypedream', 'pypedream.tests'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: BSD License',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          #'Topic :: Scientific/Engineering',
      ],
      license="BSD",
      #scripts=['scripts/pypedream',
      #        ],
      #install_requires=requires,
      #extras_require={
      #    'docs': [l.strip() for l in open('docs/build_requirements.txt')]
      #}
      )
