from distutils.core import setup

setup(
    name='ziggy',
    version='1.0.2',
    packages=['ziggytools', 'ziggytools.tests'],
    url='',
    license='',
    author='PT',
    author_email='peter.tenenbaum@nasa.gov',
    install_requires=['h5py', 'numpy'],
    description='Python packages used by Ziggy'
)
