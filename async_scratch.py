import asyncio

import websockets

async def get_bitonic_async():
    bitonic_address = "wss://api.bl3p.eu/1/BTCEUR/trades"
    async with websockets.connect(bitonic_address) as websocket:
        counter = 0

        while counter < 3:
            message = await websocket.recv()
            print(message)
            counter += 1



loop = asyncio.get_event_loop()
loop.run_until_complete(get_bitonic_async())