import rabbitpy
import time
import threading
from random import randrange

def on_message(msg):
    print(msg.body)


class Consumer(threading.Thread):
    def __init__(self, threadID, name, counter, conn):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.conn = conn

    def run(self):
        if not self.conn:
            self.conn = rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2F')
        chan = self.conn.channel()
        queue = rabbitpy.Queue(chan, "teste_mira{}".format(self.counter), durable=True)
        queue.declare()

        try:
            # Consume the message
            for message in queue:
                time.sleep(0.05)
                message.ack()

        except KeyboardInterrupt:
            print('Exited consumer')
        chan.close()


def publisher(conn):
    if not conn:
        conn = rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2F')
    chan = conn.channel()
    for i in range(10000):
        routing_key = "teste_mira{}".format(randrange(10))
        message = rabbitpy.Message(chan, '{}'.format(i))
        message.publish('', routing_key)

    print("foi")


if __name__ == "__main__":
    conn = rabbitpy.Connection('amqp://guest:guest@localhost:5672/%2F')
    conn = None
    for i in range(10):
        pub = Consumer(i, "Consumer-{}".format(i), i, conn)
        pub.start()
    
    publisher(conn)
    while True:
        time.sleep(1)
    input("rodando")