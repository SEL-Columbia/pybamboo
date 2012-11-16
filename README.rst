pybamboo
========

.. image:: https://secure.travis-ci.org/modilabs/pybamboo.png?branch=master
  :target: http://travis-ci.org/modilabs/pybamboo

Python client for the bamboo_ webservice.

.. _bamboo: http://bamboo.io/

Installation
------------

via pip

``$ pip install pybamboo``

Testing
-------

clone repository

::

    $ git clone git://github.com/modilabs/pybamboo.git
    $ cd pybamboo


install requirements

::

    $ pip install -r requirements.pip
    $ pip install -r requirements-test.pip

run tests

::

    $ cd pybamboo
    $ nosetests --with-cov --cov-report term-missing

About
-----

Bamboo is an open source project. The project features, in chronological order,
the combined efforts of

* Renaud Gaudin
* Peter Lubell-Doughtie
* Mark Johnston

and other developers.
