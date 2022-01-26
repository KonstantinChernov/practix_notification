import os

from jaeger_client import Config

conf = {
    'sampler': {
        'type': 'const',
        'param': 1,
    }
}


def setup_jaeger():
    config = Config(
        config=conf,
        service_name=os.getenv('APP_HOST'),
        validate=True,
    )
    return config.initialize_tracer()


tracer = setup_jaeger()