#!/usr/bin/env python3
"""Local-cluster delivery demos. Removes only Notify's HPA temporarily and restores it in finally.
Use NodePort URLs on Docker Desktop; with policy-capable minikube use kubectl port-forward.
"""
import argparse
import concurrent.futures
import json
import os
import subprocess
import time
import threading
import urllib.request
import urllib.error
import uuid

parser = argparse.ArgumentParser()
parser.add_argument('demo', choices=['flow', 'outage', 'rebalance', 'mongo'])
args = parser.parse_args()
context = os.environ.get('CLUSTER', 'minikube')
namespace = 'geovannycode'
urls = {name: os.environ.get(name.upper() + '_URL', f'http://localhost:{port}/services-{suffix}')
        for name, port, suffix in [('inventory', 30080, 'inventory'), ('order', 30081, 'order'), ('notify', 30082, 'notify')]}

def kubectl(*arguments, data=None):
    return subprocess.check_output(['kubectl', '--context', context, '-n', namespace, *arguments], input=data, text=True)

def request(service, path, method='GET', body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(urls[service] + path, data=data, method=method, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=5 if method == 'GET' else 40) as response:
        return json.load(response)

def wait_for(description, condition, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            print(description, flush=True)
            return
        time.sleep(1)
    raise AssertionError('Tiempo agotado: ' + description)

def pods():
    items = json.loads(kubectl('get', 'pods', '-l', 'app.kubernetes.io/name=service-notify', '-o', 'json'))['items']
    return [pod for pod in items if not pod['metadata'].get('deletionTimestamp')]

def ready(pod):
    return any(c['type'] == 'Ready' and c['status'] == 'True' for c in pod['status'].get('conditions', []))

def scale(count):
    print(kubectl('scale', 'deployment/service-notify', f'--replicas={count}'), end='', flush=True)
    if count:
        print(kubectl('rollout', 'status', 'deployment/service-notify', '--timeout=180s'), end='', flush=True)
        wait_for(f'{count} pods activos Ready', lambda: len(pods()) == count and all(ready(p) for p in pods()))
    else:
        wait_for('Notification detenido', lambda: not json.loads(kubectl('get', 'pods', '-l', 'app.kubernetes.io/name=service-notify', '-o', 'json'))['items'])

def confirm(code):
    order = request('order', '/orders', 'POST', {'codeProduct': code, 'quantity': 1})
    result = request('order', '/orders/' + str(order['id']), 'PUT')
    assert result['status'].lower() == 'completed', result
    return order['id']

def check_notifications(orders):
    def delivered():
        for order in orders:
            try:
                notifications = request('notify', '/notify?orderId=' + str(order))
            except (TimeoutError, ConnectionError, urllib.error.URLError):
                # During a rollout the NodePort may briefly still target a terminating endpoint.
                return False
            assert len(notifications) <= 1, f'Duplicados para orden {order}: {notifications}'
            if not notifications or notifications[0]['status'] != 'sent':
                return False
        return True
    wait_for(f'{len(orders)} órdenes -> {len(orders)} notificaciones sent, sin duplicados', delivered)
    # Check again after the group drains, catching delayed duplicate deliveries too.
    time.sleep(5)
    assert delivered()

saved_hpa = kubectl('get', 'hpa', 'service-notify', '-o', 'json')
hpa = json.loads(saved_hpa)
hpa.pop('status', None)
for key in ['uid', 'resourceVersion', 'creationTimestamp', 'managedFields']:
    hpa['metadata'].pop(key, None)
original_replicas = json.loads(kubectl('get', 'deployment', 'service-notify', '-o', 'json'))['spec']['replicas']
original_mongo = json.loads(kubectl('get', 'statefulset', 'mongodb', '-o', 'json'))['spec']['replicas']
orders = []
try:
    kubectl('delete', 'hpa', 'service-notify')
    scale(1)
    count = 1 if args.demo == 'flow' else 10 if args.demo in ['outage', 'mongo'] else 30
    code = 'K8S-' + str(uuid.uuid4()).upper()
    request('inventory', '/inventories', 'POST', {'idProduct': code, 'nameProduct': 'Demo Kubernetes', 'price': 1, 'stock': count})
    if args.demo == 'outage':
        scale(0)
    if args.demo == 'mongo':
        before = {p['metadata']['uid']: sum(c['restartCount'] for c in p['status']['containerStatuses']) for p in pods()}
        # Keep the outage long enough to cross the health/probe window. The PVC is retained.
        kubectl('scale', 'statefulset/mongodb', '--replicas=0')
        wait_for('Notification NotReady por MongoDB (sin reinicio)', lambda: bool(pods()) and all(not ready(p) for p in pods()))
        during = {p['metadata']['uid']: sum(c['restartCount'] for c in p['status']['containerStatuses']) for p in pods()}
        assert before == during, (before, during)
    if args.demo == 'rebalance':
        started = threading.Event()
        def produce_during_rebalance():
            confirmed = []
            for _ in range(count):
                confirmed.append(confirm(code))
                started.set()
                time.sleep(0.5)
            return confirmed
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            workload = executor.submit(produce_during_rebalance)
            assert started.wait(45), "No se pudo confirmar la primera orden"
            scale(3)
            orders = workload.result()
        print(kubectl('exec', 'kafka-0', '--', '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', 'kafka:9092',
                      '--describe', '--group', 'service-notify', '--members', '--verbose'), flush=True)
    else:
        orders = [confirm(code) for _ in range(count)]
    if args.demo == 'outage':
        scale(1)
    if args.demo == 'mongo':
        kubectl('scale', 'statefulset/mongodb', '--replicas=1')
        kubectl('rollout', 'status', 'statefulset/mongodb', '--timeout=180s')
        wait_for('Notification Ready al regresar MongoDB', lambda: bool(pods()) and all(ready(p) for p in pods()))
        after = {p['metadata']['uid']: sum(c['restartCount'] for c in p['status']['containerStatuses']) for p in pods()}
        assert before == after, (before, after)
    check_notifications(orders)
    print(json.dumps({'demo': args.demo, 'orders': orders, 'verified': True}), flush=True)
finally:
    kubectl('scale', 'statefulset/mongodb', f'--replicas={original_mongo}')
    scale(original_replicas)
    kubectl('apply', '-f', '-', data=json.dumps(hpa))
