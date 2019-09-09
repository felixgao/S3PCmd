#!/usr/bin/env python

#
# Copyright 2012-2018 BloomReach, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
S3P command line tool, setup.py
"""

import os
import stat

from setuptools import find_packages, setup
from setuptools.command.install import install as _install

__author__ = "Felix Gao"
__copyright__ = "Copyright 2012-2018 Intuit, Inc."
__license__ = "http://www.apache.org/licenses/LICENSE-2.0"
__version__ = "0.1.1"
__maintainer__ = "Felix Gao"
__status__ = "Development"

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
  long_description = f.read()

class install(_install):
  def run(self):
    _install.run(self)
    
setup(name='s3pcmd',
      version=__version__,
      description='S3P command line tool',
      author=__author__,
      license=__license__,
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/felixgao/S3PCmd.git',
      py_modules=['s3pcmd'],
      scripts=['s3pcmd.py'], 
      install_requires=['click', 'boto3>=1.3.1', 'pytz>=2016.4', 'loguru>=0.3.0', 'boto3_type_annotations>=0.3.1'],
      entry_points={
        'console_scripts': [
            's3pcmd = s3pcmd:main',
        ]},
      cmdclass={'install': install},
    )
