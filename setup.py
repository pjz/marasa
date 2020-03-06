# bootstrap if we need to
try:
        import setuptools  # noqa
except ImportError:
        from ez_setup import use_setuptools
        use_setuptools()

from setuptools import setup, find_packages

classifiers = [ 'Development Status :: 5 - Production/Stable'
              , 'Environment :: WWW'
              , 'Intended Audience :: Developers'
              , 'Intended Audience :: System Administrators'
              , 'Natural Language :: English'
              , 'Operating System :: POSIX'
              , 'Programming Language :: Python :: 3.7'
              , 'Programming Language :: Python :: Implementation :: CPython'
              ]

setup( author = 'Paul Jimenez'
     , author_email = 'pj@place.org'
     , classifiers = classifiers
     , description = 'A Log Oriented Database'
     , name = 'marasa'
     , url = 'http://github.com/pjz/marasa'
     , version = '0.1.0'
     , packages = find_packages()
     , include_package_data=True
     , entry_points = { 'console_scripts': ['npt = npt.cli:cli' ] }
     , install_requires = [ 'orjson' ]
     , extras_require = { 'dev': [ 'pytest', 'pytest-mypy', 'pytest-pylint'] }
     , zip_safe = False
      )

