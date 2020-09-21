import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='kydb',
    packages=['kydb', 'kydb.impl'],
    version='0.4.0',
    license='MIT',
    description='kydb (Kinyu Database). NoSQL DB interface.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Tony Yum',
    author_email='tony.yum@tayglobal.com',
    url='https://github.com/tayglobal/kydb',
    keywords=['NoSQL', 'Database', 'DB'],
    install_requires=[            # I get to this in a second
        'boto3',
        'redis'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
    ],
    python_requires='>=3.6',
)
