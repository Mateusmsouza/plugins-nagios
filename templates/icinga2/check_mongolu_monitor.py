#!/usr/bin/python3
import sys, argparse

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure

STATUS = {
    'OK': 0,
    'CRITICAL': 2
}

PRIMARY_INSTANCE = 1
SECONDARY_INSTANCE = 2

def get_instance_status(connection) -> int:
    return connection.admin.command( { 'replSetGetStatus': 1 } )['myState']

class ReplicaUnhealthy(Exception):
    '''raised when a replica has status different then PRIMARY_INSTANCE and SECONDARY_INSTANCE'''

def report_instance_status(user: str, password: str) -> str:
    '''
    Check if replica is healthy.

    Raises:
        - ReplicaUnhealthy: raised when replica is not healthy
    Usage:
        >>> report_instance_status(root, passwd)
        >>> Ok - Replica Secondary|Replica Health=1;;0;;
    '''
    connection = MongoClient(
        username=user,
        password=password)
    message = '|replica_health=1;;0;;'
    instance_status = get_instance_status(connection)
    if instance_status == PRIMARY_INSTANCE:
        message = f'Ok - Replica Primary{message}'
    elif instance_status == SECONDARY_INSTANCE:
        message = f'Ok - Replica Secondary{message}'
    else:
        message = f'Critical - Replica NOK!{message}'
        raise ReplicaUnhealthy(message)
    return message

parser = argparse.ArgumentParser(description='Monitoring tool for MongoDBs. Written by Mateus Souza.')
parser.add_argument('-u',
    '--user',
    action='store',
    type=str,
    required=True,
    help='MongoDB root user')
parser.add_argument('-p',
    '--password',
    action='store',
    type=str,
    required=True,
    help='MongoDB password')

def main():
    try:
        args = parser.parse_args()
        message = report_instance_status(args.user, args.password)
        print(message)
        sys.exit(STATUS['OK'])
    except ServerSelectionTimeoutError as ex:
        print(*ex.args)
        sys.exit(STATUS['CRITICAL'])
    except OperationFailure as ex:
        print(*ex.args)
        sys.exit(STATUS['CRITICAL'])
    except ReplicaUnhealthy as ex:
        print(*ex.args)
        sys.exit(STATUS['CRITICAL'])

if __name__ == '__main__':
    main()
