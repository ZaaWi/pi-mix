#!/usr/bin/env python3
"""Run on the Pi as root: back up state, pause broken sync, restore UART."""
import datetime
import json
import pathlib
import subprocess


def kubectl(*args):
    return subprocess.check_output(["kubectl", *args], text=True)


def main():
    backup = pathlib.Path('/var/lib/pi-mix-maintenance') / datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    backup.mkdir(parents=True, mode=0o700)
    for namespace in ('iot', 'database', 'argocd'):
        resources = 'applications' if namespace == 'argocd' else 'deployments,statefulsets,services,persistentvolumeclaims,configmaps'
        (backup / (namespace + '.json')).write_text(kubectl('get', resources, '-n', namespace, '-o', 'json'))
    with (backup / 'appdb.sql.partial').open('w') as out:
        result = subprocess.run(['kubectl', 'exec', '-n', 'database', 'postgres-0', '--', 'pg_dump', '-U', 'appuser', '-d', 'appdb', '--no-owner', '--no-acl'], stdout=out)
    if result.returncode == 0:
        (backup / 'appdb.sql.partial').rename(backup / 'appdb.sql')
    else:
        print('Database unavailable: database migration deferred; changing only Pi live collection.')
    kubectl('patch', 'application', 'iot', '-n', 'argocd', '--type=merge', '-p', json.dumps({'spec': {'syncPolicy': {'automated': {'enabled': False}}}}))
    app = json.loads(kubectl('get', 'application', 'iot', '-n', 'argocd', '-o', 'json'))
    if app.get('status', {}).get('operationState', {}).get('phase') == 'Running':
        kubectl('patch', 'application', 'iot', '-n', 'argocd', '--type=merge', '-p', json.dumps({'status': {'operationState': {'phase': 'Terminating'}}}))
    patch = {'spec': {'strategy': {'type': 'Recreate', 'rollingUpdate': None}, 'template': {'spec': {'nodeSelector': {'kubernetes.io/hostname': 'the-bear'}, 'containers': [{'name': 'bridge', 'env': [{'name': 'BRIDGE_BAUD', 'value': '9600'}]}]}}}}
    kubectl('patch', 'deployment', 'serial-bridge', '-n', 'iot', '--type=strategic', '-p', json.dumps(patch))
    print('Backups:', backup)
    print(kubectl('rollout', 'status', 'deployment/serial-bridge', '-n', 'iot', '--timeout=90s'))


if __name__ == '__main__':
    main()
