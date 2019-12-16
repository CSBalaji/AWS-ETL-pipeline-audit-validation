"""DynamoDB functions
1. connect to dynamodb and lookup credentials
2. get plain text credentials
"""
import base64
import sys
import warnings
import logging

import boto3


def get_cred_from_ddb(env_tbl, env_name):
    warnings.filterwarnings("ignore", message="unclosed.*<ssl.SSLSocket.*>")
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    try:

        dynamodb = boto3.resource("dynamodb", region_name='us-east-1')
        table = dynamodb.Table(env_tbl)
        response = table.get_item(Key={"env_name": env_name})
        return response['Item']
    except Exception as exception:
        logging.exception(exception)
        pass


def get_passwd(pw):
    warnings.filterwarnings("ignore", message="unclosed.*<ssl.SSLSocket.*>")
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    try:
        # decrypt Password
        # kms = boto3.client('kms')
        # password = kms.decrypt(CiphertextBlob=base64.b64decode(pw))
        # return password["Plaintext"].decode('utf-8')
        kms = boto3.client('kms')
        password = kms.decrypt(CiphertextBlob=base64.b64decode(pw))
        return password["Plaintext"].decode('utf-8')

    except Exception as exception:
        logging.exception(exception)
        pass
