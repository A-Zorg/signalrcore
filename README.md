# SignalR core client

This signalr core client is forked from mandrewcito. The main difference is the replacement of the synchronous ```websocket-client``` library by the asynchronous ```websockets``` library. Additionally, all methods have been made asynchronous.

See https://github.com/mandrewcito/signalrcore for a general introduction.

See the following samples to get an idea of the changes:

```python
import asyncio

from signalrcore_async.hub_connection_builder import HubConnectionBuilder
from signalrcore_async.protocol.msgpack import MessagePackHubProtocol

async def main():

    protocol = "ws"
    host = "localhost"
    port = "8080"
    hub = "hub"
    hub_url = f"{protocol}://{host}:{port}/{hub}"
    
    connection = HubConnectionBuilder()\
                .with_url(hub_url)\
                # optional: use MessagePack instead of json protocol
                .with_hub_protocol(MessagePackHubProtocol())\
                .build()

    try:
        # start connection
        await connection.start()

        # send (fire and forget)
        connection.send("SendName", "R2D2")

        # invoke (wait for return value)
        sum_value = await connection.invoke("Sum", [1, 2])
        print(sum_value)

        # register callback
        connection.on("OnProgressChanged", _on_progress_changed)

        # stream
        await connection.stream("StreamData", [1, 2], _on_next)

    finally:
        # close connection
        await connection.stop()

def _on_next(data):
    pass # do something with the streamed data

def _on_progress_changed(self, args):
    progress = args[0]
    print(f"Progress: {progress * 100:.0f}%")

# run main task
asyncio.run(main())
```