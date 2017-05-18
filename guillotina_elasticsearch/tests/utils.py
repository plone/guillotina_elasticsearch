from guillotina.tests import utils
from time import sleep

import docker
import json
import os
import requests


ELASTICSEACH_IMAGE = 'elasticsearch:5.2.0'


def run_elasticsearch_docker(label='testing'):
    docker_client = docker.from_env(version='1.23')

    test_containers = docker_client.containers.list(
        all=True,
        filters={'label': label})
    for test_container in test_containers:
        test_container.stop()
        test_container.remove(v=True, force=True)
    container = docker_client.containers.run(
        image=ELASTICSEACH_IMAGE,
        labels=[label],
        detach=True,
        ports={
            '9200/tcp': 9200
        },
        cap_add=['IPC_LOCK'],
        mem_limit='1g',
        environment={
            'cluster.name': 'docker-cluster',
            'bootstrap.memory_lock': True,
            'ES_JAVA_OPTS': '-Xms512m -Xmx512m'
        }
    )
    ident = container.id
    count = 1
    container = docker_client.containers.get(ident)

    opened = False
    url = ''

    print('starting elasticsearch')
    while count < 30 and not opened:
        count += 1
        container = docker_client.containers.get(ident)
        sleep(1)
        if container.attrs['NetworkSettings']['IPAddress'] != '':
            if os.environ.get('TESTING', '') == 'jenkins':
                url = 'http://' + container.attrs['NetworkSettings']['IPAddress'] + ':9200'  # noqa
            else:
                url = 'http://localhost:9200/'

        if url != '':
            try:
                resp = requests.get(url)
                if resp.status_code == 200:
                    print('successfully connected to elasticsearch')
                    opened = True
            except: # noqa
                resp = None
    print('elasticsearch started {}'.format(container.attrs['NetworkSettings']['IPAddress']))
    return container


async def add_content(requester, num_folders=10, num_items=10, base_id='es-'):
    path = '/db/guillotina/'
    created = 0
    for fidx in range(num_folders):
        folder_id = f'{base_id}folder{str(fidx)}'
        resp, status = await requester(
            'POST',
            path,
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder' + str(fidx),
                'id': folder_id
            })
        )
        created += 1
        assert status == 201
        path += '/' + folder_id
        for idx in range(num_items):
            resp, status = await requester(
                'POST',
                path,
                data=json.dumps({
                    '@type': 'Example',
                    'title': 'Item' + str(idx)
                })
            )
            created += 1
            assert status == 201
    return created


async def setup_txn_on_container(requester):
    request = utils.get_mocked_request(requester.db)
    utils.login(request)
    container = await utils.get_container(request=request)
    request.container = container

    tm = request._tm
    txn = await tm.begin(request)
    return container, request, txn, tm
