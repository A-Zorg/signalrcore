import uuid
import logging
from .hub.base_hub_connection import BaseHubConnection
from .hub.auth_hub_connection import AuthHubConnection
from .hub.reconnection import \
    IntervalReconnectionHandler, RawReconnectionHandler, ReconnectionType
from .helpers import Helpers
from .messages.invocation_message import InvocationMessage
from .protocol.json_hub_protocol import JsonHubProtocol
from .protocol.messagepack_protocol import MessagepackProtocol
from .subject import Subject



class HubConnectionError(ValueError):
    pass


class HubConnectionBuilder(object):
    """
    Hub connection class, manages handshake and messaging

    Args:
        hub_url: SignalR core url

    Raises:
        HubConnectionError: Raises an Exception if url is empty or None
    """
    def __init__(self):
        self.hub_url = None
        self.hub = None
        self.options = {
                "access_token_factory": None
            }
        self.token = None
        self.headers = None
        self.negotiate_headers = None
        self.has_auth_configured = None
        self.protocol = None
        self.reconnection_handler = None
        self.keep_alive_interval = None
        self.verify_ssl = True
        self.skip_negotiation = False # By default do not skip negotiation

    def with_url(
            self,
            hub_url,
            options=None):
        if hub_url is None or hub_url.strip() is "":
            raise HubConnectionError("hub_url must be a valid url.")

        if options is not None and type(options) != dict:
            raise HubConnectionError(
                "options must be a dict {0}.".format(self.options))

        if options is not None \
                and "access_token_factory" in options.keys()\
                and not callable(options["access_token_factory"]):
            raise HubConnectionError(
                "access_token_factory must be a function without params")

        if options is not None:

            self.has_auth_configured = \
                "access_token_factory" in options.keys()\
                and callable(options["access_token_factory"])

            self.skip_negotiation = "skip_negotiation" in options.keys() and options["skip_negotiation"]

        self.hub_url = hub_url
        self.hub = None
        self.options = self.options if options is None else options
        return self

    def configure_logging(self, logging_level, handler=None):
        """
        Confiures signalr logging
        :param handler:  custom logging handler
        :param socket_trace: Enables socket package trace
        :param logging_level: logging.INFO | logging.DEBUG ... from python logging class
        :param log_format: python logging class format by default %(asctime)-15s %(clientip)s %(user)-8s %(message)s
        :return: instance hub with logging configured
        """
        Helpers.configure_logger(logging_level, handler)
        return self

    def build(self):
        """"
        self.token = token
        self.headers = headers
        self.negotiate_headers = negotiate_headers
        self.has_auth_configured = token is not None

        """
        # self.protocol = JsonHubProtocol()
        self.protocol = MessagepackProtocol()
        self.headers = {}

        if "headers" in self.options.keys() and type(self.options["headers"]) is dict:
            self.headers = self.options["headers"]

        if self.has_auth_configured:
            auth_function = self.options["access_token_factory"]
            if auth_function is None or not callable(auth_function):
                raise HubConnectionError(
                    "access_token_factory is not function")
        if "verify_ssl" in self.options.keys() and type(self.options["verify_ssl"]) is bool:
            self.verify_ssl = self.options["verify_ssl"]

        self.hub = AuthHubConnection(
            self.hub_url,
            self.protocol,
            auth_function,
            keep_alive_interval=self.keep_alive_interval,
            reconnection_handler=self.reconnection_handler,
            headers=self.headers,
            verify_ssl=self.verify_ssl,
            skip_negotiation=self.skip_negotiation)\
            if self.has_auth_configured else\
            BaseHubConnection(
                self.hub_url,
                self.protocol,
                keep_alive_interval=self.keep_alive_interval,
                reconnection_handler=self.reconnection_handler,
                headers=self.headers,
                verify_ssl=self.verify_ssl,
                skip_negotiation=self.skip_negotiation)

        return self

    def with_automatic_reconnect(self, data):
        """
        https://devblogs.microsoft.com/aspnet/asp-net-core-updates-in-net-core-3-0-preview-4/
        :param data:
        :return:
        """
        reconnect_type = data["type"] if "type" in data.keys() else "raw"

        max_attempts = data["max_attempts"] if "max_attempts" in data.keys() else None # Infinite reconnect

        reconnect_interval = data["reconnect_interval"]\
            if "reconnect_interval" in data.keys() else 5 # 5 sec interval
        
        keep_alive_interval = data["keep_alive_interval"]\
            if "keep_alive_interval" in data.keys() else 15

        intervals = data["intervals"]\
            if "intervals" in data.keys() else []  # Reconnection intervals

        self.keep_alive_interval = keep_alive_interval

        reconnection_type = ReconnectionType[reconnect_type]

        if reconnection_type == ReconnectionType.raw:
            self.reconnection_handler = RawReconnectionHandler(
                reconnect_interval,
                max_attempts
            )
        if reconnection_type == ReconnectionType.interval:
            self.reconnection_handler = IntervalReconnectionHandler(
                intervals
            )
        return self

    def on_close(self, callback):
        self.hub.on_disconnect = callback

    def on_open(self, callback):
        self.hub.on_connect = callback

    def on(self, event, callback_function):
        """
        Register a callback on the specified event
        :param event: Event name
        :param callback_function: callback function, arguments will be binded
        :return:
        """
        self.hub.register_handler(event, callback_function)

    async def stream(self, event, event_params, on_next_item):
        await self.hub.stream(event, event_params, on_next_item)

    async def start(self):
        await self.hub.start()

    async def stop(self):
        await self.hub.stop()

    async def invoke(self, method, arguments):
        if type(arguments) is not list:
            raise HubConnectionError("Arguments of a message must be a list")

        if type(arguments) is list:
            invocation_id = str(uuid.uuid4())
            message = InvocationMessage({}, invocation_id, method, arguments)
            return await self.hub.invoke(message)

    def send(self, method, arguments):
        if type(arguments) is not list and type(arguments) is not Subject:
            raise HubConnectionError("Arguments of a message must be a list or subject")

        if type(arguments) is list:
            self.hub.send(InvocationMessage(
                {},
                0,
                method,
                arguments))

        if type(arguments) is Subject:
            arguments.connection = self
            arguments.target = method
            arguments.start()
