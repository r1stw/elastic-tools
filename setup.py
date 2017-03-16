from setuptools import setup, find_packages

setup(name='elastictools',
      version='0.0.1',
      description='Elastic search requests wrapper',
      url='https://github.com/pleskanovsky/elastictools',
      author='pleskanovsky',
      author_email='vnezapno.pochta@gmail.com',
      license='MIT',
      install_requires="elasticsearch",
      packages=find_packages(),
      zip_safe=False)
