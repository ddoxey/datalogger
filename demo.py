#!/usr/bin/env python3
import re
import time
import argparse
import subprocess
from datetime import datetime
from datalogger import DataLogger, DataCollector
from pprint import pprint


def non_empyt_lines(text):
    if text is None:
        return []
    return [line.strip() for line in text.split("\n")
                         if len(line.strip()) > 0]


def shell_command(cmd_tokens):
    proc = subprocess.run(cmd_tokens,
                    encoding='UTF-8',
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=False)
    result = {
        'status': int(proc.returncode),
        'stdout': non_empyt_lines(proc.stdout),
        'stderr': non_empyt_lines(proc.stderr),
    }
    return result


def compute_elapsed(mon, day, hhmmss, year):
    format_s = '%b, %d, %H:%M:%S, %Y'
    date_s = f'{mon}, {day}, {hhmmss}, {year}'
    epoch = datetime(1970, 1, 1)
    return (datetime.strptime(date_s, format_s) - epoch).total_seconds()


def get_procs():
    result=shell_command(['ps', '-eo', 'pid,pcpu,pmem,lstart,cmd'])
    if result['status'] != 0:
        return None
    records = []
    header = None
    for line in result['stdout']:
        if header is None:
            header=line
        else:
            line = re.sub(r'\s+', ' ', line)
            pid, cpu, mem, _, mon, day, hhmmss, year, command=line.split(' ', 8)
            runtime = compute_elapsed(mon, day, hhmmss, year)
            records.append({
                'PID': int(pid),
                'CPU': float(cpu),
                'MEM': float(mem),
                'COMMAND': command,
                'RUNTIME': int(runtime)})
    if len(records) == 0:
        return None
    return records


def get_network():
    result=shell_command(['ss', '-n'])
    if result['status'] != 0:
        return None
    records = []
    header=None
    for line in result['stdout']:
        if header is None:
            header=line
        else:
            line=re.sub(r'\s+', ' ', line)
            print(f'Line >{line}<')
            if ':' in line:
                netid, state, recvq, sendq, local, peer=line.split(' ', 5)
                local_address, local_port = local.split(':')
                peer_address, peer_port = peer.split(':')
            else:
                netid, state, recvq, sendq, \
                    local_address, local_port, \
                        peer_address, peer_port = line.split(' ', 7)
            records.append({
                'Netid': netid,
                'State': state,
                'RecvQ': int(recvq),
                'SendQ': int(sendq),
                'LocalAddress': local_address,
                'LocalPort': int(local_port),
                'PeerAddress': peer_address,
                'PeerPort': int(peer_port)})
    if len(records) == 0:
        return None
    return records


def run(export_fh):

    log = DataLogger('logger')

    log.register(
        DataCollector('procs',
                      get_procs,
                      {'PID': int,
                       'CPU': float,
                       'MEM': float,
                       'COMMAND': str,
                       'RUNTIME': int}),
        DataCollector('network',
                      get_network,
                      {'Netid': str,
                       'State': str,
                       'RecvQ': int,
                       'SendQ': int,
                       'LocalAddress': str,
                       'LocalPort': int,
                       'PeerAddress': str,
                       'PeerPort': int}))

    if export_fh is not None:
        pprint(export_fh)
        db_filename = log.last_db('logger')
        print(f'Exporting {db_filename} to: {export_fh[0].name}')
        if log.export(export_fh[0]):
            return 0
        return 1

    while True:
        log.update()
        time.sleep(1)

    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='System Logger',
                                     description='Log some system data')
    parser.add_argument('--export', nargs=1, type=argparse.FileType('w'))
    args = parser.parse_args()

    run(args.export)
