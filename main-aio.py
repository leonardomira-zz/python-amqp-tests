import asyncio
import aio_pika
from random import randrange


async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        #print(message.body)
        await asyncio.sleep(0.05)


async def main(loop, queue_name):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop
    )

    #queue_name = "teste_mira"

    # Creating channel
    channel = await connection.channel()

    # Maximum message count which will be
    # processing at the same time.
    await channel.set_qos(prefetch_count=100)

    # Declaring queue
    queue = await channel.declare_queue(
        queue_name, auto_delete=False, durable=True
    )

    await queue.consume(process_message)

    return connection


async def pub(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop)

    async with connection:
        channel = await connection.channel()
        for i in range(10000):
            #print(i)
            routing_key = "teste_mira{}".format(randrange(10))

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body='Hello {}'.format(routing_key).encode()
                ),
                routing_key=routing_key
            )
        print('foi')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection0 = loop.run_until_complete(main(loop, 'teste_mira0'))
    connection1 = loop.run_until_complete(main(loop, 'teste_mira1'))
    connection2 = loop.run_until_complete(main(loop, 'teste_mira2'))
    connection3 = loop.run_until_complete(main(loop, 'teste_mira3'))
    connection4 = loop.run_until_complete(main(loop, 'teste_mira4'))
    connection5 = loop.run_until_complete(main(loop, 'teste_mira5'))
    connection6 = loop.run_until_complete(main(loop, 'teste_mira6'))
    connection7 = loop.run_until_complete(main(loop, 'teste_mira7'))
    connection8 = loop.run_until_complete(main(loop, 'teste_mira8'))
    connection9 = loop.run_until_complete(main(loop, 'teste_mira9'))
    connectionp = loop.run_until_complete(pub(loop))

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection0.close())
        loop.run_until_complete(connection1.close())
        loop.run_until_complete(connection2.close())
        loop.run_until_complete(connection3.close())
        loop.run_until_complete(connection4.close())
        loop.run_until_complete(connection5.close())
        loop.run_until_complete(connection6.close())
        loop.run_until_complete(connection7.close())
        loop.run_until_complete(connection8.close())
        loop.run_until_complete(connection9.close())