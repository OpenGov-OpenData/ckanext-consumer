from setuptools import setup, find_packages

setup(
    name='ckanext-consumer',
    version='0.1.0',
    description='Event-driven architecture extension for CKAN (Kafka Consumer)',
    long_description='Listens to Kafka topics and dispatches events to registered handlers in other CKAN extensions.',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    author='Peter Vorman',
    author_email='pvorman@opengov.com',
    url='https://github.com/OpenGov-OpenData/ckanext-consumer',
    license='GNU AFFERO GENERAL PUBLIC LICENSE',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points='''
        [ckan.plugins]
        consumer=ckanext.consumer.plugin:ConsumerPlugin

        [babel.extractors]
        ckan = ckan.lib.extract:extract_ckan
    ''',
)
