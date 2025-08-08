from typing import Optional
from opentelemetry import baggage
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from modules.pull_router.router_api import ROUTING_KEY, FILTER_ATTRIBUTE_NAME

def extract_routing_key_from_baggage(baggage_header_value: str) -> Optional[str]:

    if not baggage_header_value:
        return None

    carrier = {FILTER_ATTRIBUTE_NAME: baggage_header_value}
    ctx = W3CBaggagePropagator().extract(carrier=carrier)

    return baggage.get_baggage(ROUTING_KEY, ctx)