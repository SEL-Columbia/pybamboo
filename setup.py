from distutils.core import setup

setup(
     name='pybamboo',
     version='0.1.7',
     author='modilabs',
     author_email='info@modilabs.org',
     packages=['pybamboo'],
     package_dir={'pybamboo': 'pybamboo'},
     url='http://pypi.python.org/pypi/pybamboo/',
     description='A Python package to interact with bamboo.io.',
     long_description=open('README.rst', 'rt').read(),
     install_requires=[
        'requests==0.14.0',
        'simplejson==2.6.2',
    ],
)
