#!/usr/bin/python3
"""Monitoring tool for Redis

Written by Mateus Souza

Currently it checks:
- Redis Memory Usage. If greather than memory usage threshold an alert is raised
- Redis Latency. If greather than latency threshold an alert is raised
"""

import argparse, sys, subprocess
from typing import Tuple
import redis


STATUS = {
    'OK': 0,
    'CRITICAL': 2
}

ARGS = {
    'MAX_PERCENTUAL_MEMORY_USAGE': 0.95,
    'MAX_LATENCY_MS': 20.0
}

parser = argparse.ArgumentParser(description='Monitoring tool for Redis. Written by Mateus Souza.')
parser.add_argument('-m',
                       '--max-memory-usage',
                       action='store',
                       type=float,
                       help='percentual max memory to alert. Default is 0.95')

parser.add_argument('-l',
                       '--max-latency',
                       action='store',
                       type=float,
                       help='max latency in millisecond to alert. Default is 20.0')


def run_keydb_latency_check() -> None:
    keydb_latency_command = "keydb-cli --latency"
    process = subprocess.Popen(keydb_latency_command.split(), stdout=subprocess.PIPE)
    output, _ = process.communicate()
    return output.decode('utf-8').split(' ')

def format_message(
    memory_healthy: bool,
    max_memory_allowed_human: str,
    latency_healthy: bool,
    max_latency: str,
    client_information: dict,
    latency: float) -> str:
    used_memory_human = f'{client_information["used_memory_human"]}B'
    maxmemory_human = f'{client_information["maxmemory_human"]}B'

    base_massage = f"{'high' if not memory_healthy else ''} memory usage: " \
            f"{used_memory_human} used of {maxmemory_human} " \
            f"/ {'high' if not latency_healthy else ''} average latency: {latency} ms"

    return f"{base_massage}|used_memory={used_memory_human};;{max_memory_allowed_human};; average_latency={latency};;{max_latency};;"

def get_memory_threshold_by_percentage(max_memory_usage: float, client_information: dict) -> float:
    max_memory_allowed = max_memory_usage * client_information['maxmemory']
    return max_memory_allowed

def get_human_memory_threshold_by_percentage(max_memory_usage: float, client_information: dict) -> float:
    maxmemory_memory_human: str  = client_information["maxmemory_human"]
    size_maxmemory_memory_human = len(maxmemory_memory_human)

    maxmemory_memory_human_value = float(maxmemory_memory_human[0:size_maxmemory_memory_human-1])
    max_memory_to_alarm: float =  maxmemory_memory_human_value * max_memory_usage
    max_memory_unit_byte: str = maxmemory_memory_human[-1:size_maxmemory_memory_human]

    max_memory_allowed_human = f'{max_memory_to_alarm:.2f}{max_memory_unit_byte}B'
    return max_memory_allowed_human

def check_memory_usage(client_information: dict, max_memory_usage: float) -> Tuple[bool, str]:
    memory_healthy = True
    max_memory_allowed = get_memory_threshold_by_percentage(max_memory_usage, client_information)
    max_memory_allowed_human = get_human_memory_threshold_by_percentage(max_memory_usage, client_information)
    if client_information['used_memory'] > max_memory_allowed:
        memory_healthy = False
    return memory_healthy, max_memory_allowed_human

def check_latency(max_latency: float) -> Tuple[bool, float]:
    results = run_keydb_latency_check()
    average_latency = float(results[2])
    if average_latency > max_latency:
        return False, average_latency
    return True, average_latency

def report_stats(max_latency: float, max_memory_usage: float) -> str:
    """
    Check Redis stats given threshold or default threshold.

    max_latency: float - latency threshold, if not passed default is 20.0 miliseconds
    max_memory_usage: float - max_memory_usage threshold, if not passed default is 0.95 which is 95%

    Raises:
        - redis.RedisError

    Usage:
    >>> report_stats(max_latency=20.0, max_memory_usage=0.95)
    >>> "memory usage: 6.02M used of 3.59G /  average latency: 0.07 ms|used_memory=6.02M;;3.41G;; average_latency=0.07;;20.0;;"
    """
    con = redis.Redis()
    client_information = con.client().info()

    if not max_memory_usage:
        max_memory_usage = ARGS['MAX_PERCENTUAL_MEMORY_USAGE']
    if not max_latency:
        max_latency = ARGS['MAX_LATENCY_MS']

    memory_healthy, max_memory_allowed_human = check_memory_usage(client_information, max_memory_usage)
    latency_healthy, latency = check_latency(max_latency)

    message = format_message(
        memory_healthy=memory_healthy,
        max_memory_allowed_human=max_memory_allowed_human,
        latency_healthy=latency_healthy,
        max_latency=max_latency,
        client_information=client_information,
        latency=latency)

    if not memory_healthy or not latency_healthy:
        raise redis.RedisError(message)

    return message

def main():
    try:
        args = parser.parse_args()
        message = report_stats(
            max_latency=args.max_latency,
            max_memory_usage=args.max_memory_usage)
        print(message)
        sys.exit(STATUS['OK'])
    except redis.RedisError as ex:
        print(*ex.args)
        sys.exit(STATUS['CRITICAL'])

if __name__ == '__main__':
    main()
