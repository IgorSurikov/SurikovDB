from typing import Optional

from aiohttp import web



class DataBaseServer:

    def __init__(self, data_base, host: Optional[str] = None, port: Optional[int] = None):
        self._data_base = data_base
        if host is None:
            host = 'localhost'

        if port is None:
            port = '3471'

        self._port = port
        self._host = host

    def run(self):
        app = web.Application()
        app.add_routes([web.get('/', self.get_data_base_info_handler),
                        web.post('/', self.post_data_base_query_handler)])
        web.run_app(app, host=self._host, port=self._port)

    async def get_data_base_info_handler(self, request):
        response = {
            'data_base_name': self._data_base.name,
            'tables': self._data_base.get_table_list()
        }

        return web.json_response(response)

    async def post_data_base_query_handler(self, request):
        json = await request.json()
        response = self._data_base.query(json)
        return web.json_response(response)
