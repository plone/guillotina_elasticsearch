from guillotina.tests import utils

import asyncio
import json


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
    await asyncio.sleep(1)  # make sure async index tasks finish
    return created


async def setup_txn_on_container(requester):
    request = utils.get_mocked_request(requester.db)
    utils.login(request)
    container = await utils.get_container(request=request)
    request.container = container

    tm = request._tm
    txn = await tm.begin(request)
    return container, request, txn, tm
