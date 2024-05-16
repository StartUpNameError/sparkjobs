from shared.connection import Connection

from ._secret import SecretConnectionFactory


def get_secret_from_sellout(sellout_id: int) -> str:
    secrets = {"85": "production/oxxo_redshift"}
    return secrets[str(sellout_id)]


class SelloutConnectionFactory:
    """Creates Connection for sellout id.

    Uses :class:`SecretConnectionFactory` internally.
    """

    def __init__(self, sellout_id: int):
        self.sellout_id = sellout_id

    def __call__(self) -> Connection:
        secret = get_secret_from_sellout(self.sellout_id)
        return SecretConnectionFactory(secret)()
