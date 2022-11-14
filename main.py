import asyncio

from register import Register


async def handler():
    _register = Register()
    await _register.worker()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handler())