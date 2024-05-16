import json

import boto3

from shared.connection import Connection


def get_secret(secret_id: str):
    secretsmanager_client = boto3.client("secretsmanager")
    secret_manager_response = secretsmanager_client.get_secret_value(
        SecretId=secret_id
    )
    return json.loads(secret_manager_response["SecretString"])


class SecretConnectionFactory:
    def __init__(self, secret_id: str) -> None:
        self.secret_id = secret_id

    def __call__(self) -> Connection:
        secret = get_secret(self.secret_id)
        return Connection.from_secret(secret=secret)
