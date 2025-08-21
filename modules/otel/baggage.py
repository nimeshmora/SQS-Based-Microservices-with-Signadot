from typing import Optional
from opentelemetry import baggage
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.propagators.textmap import Getter, Setter
from modules.pull_router.router_api import ROUTING_KEY, FILTER_ATTRIBUTE_NAME

# Custom Setter for SQS message attributes
class SQSSetter(Setter):
    def set(self, carrier, key, value):
        carrier[key] = {"StringValue": value, "DataType": "String"}

class SQSGetter(Getter):
    def get(self, carrier, key):
        if key in carrier:
            return [carrier[key]["StringValue"]]
        return []
    def keys(self, carrier):
        return carrier.keys()

# Custom Getter for HTTP headers
class HeaderGetter(Getter):
    def get(self, carrier, key):
        return [carrier.get(key)] if carrier.get(key) else []
    def keys(self, carrier):
        return carrier.keys()

# Define a simple HTTP header setter
class HeaderSetter(Setter):
    def set(self, carrier, key, value):
        carrier[key] = value

header_getter = HeaderGetter()
sqs_setter = SQSSetter()
sqs_getter = SQSGetter()

def extract_routing_key_from_baggage(message_attributes: dict) -> Optional[str]:

    ctx = W3CBaggagePropagator().extract(
        carrier=message_attributes,
        getter=sqs_getter
    )

    return baggage.get_baggage(ROUTING_KEY, ctx)

def inject_baggage_into_message_header(message_attributes: dict, ctx: baggage.Context) -> Optional[str]:

    W3CBaggagePropagator().inject(
        carrier=message_attributes,
        setter=sqs_setter,
        context=ctx
    )

    return baggage.get_baggage(ROUTING_KEY, ctx)

def extract_routing_key_from_header(header_attributes: dict) -> Optional[baggage.Context]:

    ctx = W3CBaggagePropagator().extract(
        carrier=header_attributes,
        getter=header_getter
    )

    return ctx

def inject_baggage_into_request_header(headers: dict, ctx: baggage.Context) -> Optional[str]:

    W3CBaggagePropagator().inject(
        carrier=headers,
        setter=HeaderSetter(),
        context=ctx
    )

    return baggage.get_baggage(ROUTING_KEY, ctx)