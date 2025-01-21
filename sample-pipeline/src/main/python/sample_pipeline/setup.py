from setuptools import setup

# Ordinarily the right way to configure this would be to add ziggytools to the
# install_requires list. Unfortunately, I haven't figured out how to install one
# local package that depends on another local package: there doesn't seem to be
# an obvious way to tell pip where to look.
setup(
    name='sample_pipeline',
    version='0.8.0',
    packages=['major_tom'],
    url='',
    license='',
    author='PT',
    author_email='peter.tenenbaum@nasa.gov',
    install_requires=['Pillow', 'numpy'],
    description='Python packages used by Ziggy\'s sample pipeline'
)
