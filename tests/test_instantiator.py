from shared.instantiator import Instatiator
from shared.url import URL


def test_instantiator():

    context = {
        "drivername": "drivername",
        "host": "host",
        "port": "port",
        "username": "username",
        "password": "password",
        "database": "database",
    }

    expected_instance = URL(**context)

    clspath = "shared.url.URL"
    instantiator = Instatiator(context=context)
    instance = instantiator.instantiate(clspath=clspath)

    assert instance == expected_instance


def test_instantiator_with_extra_context():

    context = {
        "drivername": "drivername",
        "host": "host",
        "port": "port",
    }

    extra_context = {
        "username": "username",
        "password": "password",
        "database": "database",
    }

    expected_instance = URL(**context, **extra_context)

    clspath = "shared.url.URL"
    instantiator = Instatiator(context=context)
    instance = instantiator.instantiate(clspath=clspath, context=extra_context)

    assert instance == expected_instance
