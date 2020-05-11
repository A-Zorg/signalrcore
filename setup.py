import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="signalrcore-async",
    version="0.5.2",
    author="Apollo3zehn",
    description="Asynchronous fork of signalrcore with MessagePack support: A Python SignalR Core client, with invocation auth and two way streaming. Compatible with azure / serverless functions. Also with automatic reconnect and manual reconnect.",
    keywords="signalr core client 3.1",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apollo3zehn/signalrcore",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    install_requires=[
        "msgpack>=1.0.0",
        "requests>=2.21.0",
        "websockets>=8.1"
    ]
)
