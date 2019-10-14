#!/usr/bin/env python
"""
pypedream - Utility library for scriptwriting
"""

#__all__ = []

__version__ = '0.0.1'
__author__ = 'Stig-Arne Gronroos'
__author_email__ = "stig-arne.gronroos@aalto.fi"



def get_version():
    return __version__

# The public api imports need to be at the end of the file,
# so that the package global names are available to the modules
# when they are imported.
# pylint: disable=C0413

from .pypedream import *
