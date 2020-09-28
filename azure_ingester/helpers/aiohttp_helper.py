import asyncio
import aiohttp
import aiohttp.web
import backoff
import json


MAX_RETRY = 5

aiohttp_excpetions = {
    aiohttp.ClientResponseError,
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    aiohttp.web.HTTPException,
    aiohttp.web.HTTPClientError
}


def backoff_hdlr(details):
    """
    Logs backoff messages with args/kwargs
    """
    print("Backing off {wait:0.1f} seconds after {tries} calling function {target} with args {args} and "
          "kwargs {kwargs}".format(**details))


@backoff.on_exception(backoff.expo,
                      aiohttp_excpetions,
                      max_tries=MAX_RETRY,
                      on_backoff=backoff_hdlr,
                      jitter=backoff.full_jitter)
async def handler(url: str, data: dict, semaphore: asyncio.Semaphore, request_type: str, api_token: str = None,
                  headers: dict = None):
    if headers is None:
        headers = {"Authorization": f"Bearer {api_token}"}
    async with semaphore,\
               aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10), raise_for_status=True) as session:
        _options = {
            "GET": session.get,
            "POST": session.post,
            "PATCH": session.patch
        }
        try:
            _req_method = _options.get(request_type)(url, headers=headers, json=data, ssl=True)
            async with _req_method as resp:
                content = await resp.read()
                return json.loads(content.decode("utf-8"))
        except aiohttp_excpetions as e:
            print(e)
            raise e


async def send_requests(request_object: list):
    semaphore = asyncio.Semaphore(50)
    tasks = []
    for r in request_object:
        _method = r.get("method")
        tasks.append(handler(r.get("url"), r.get("data"), semaphore, _method, headers=r.get("headers")))
    return await asyncio.gather(*tasks)


async def aiohttp_handler(req: list):
    breakpoint()
    response = asyncio.run(send_requests(req))
    return response
