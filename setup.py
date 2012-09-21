from distutils.core import setup

setup(
     name='pybamboo',
     version='0.1',
     author='modilabs',
     author_email='info@modilabs.org',
     packages=['bamboo'],
     package_dir={'bamboo': 'bamboo'},
     url='http://pypi.python.org/pypi/pybamboo/',
     description='A Python package to interact with bamboo.io.',
     long_description=open('README.md', 'rt').read(),
     install_requires=[
        'requests==0.14.0',
    ],
)
