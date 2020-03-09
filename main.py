import amqp
import time

def on_message(msg):
    print(msg.body)

def pub():
    conn = amqp.connection.Connection()
    conn.connect()
    channel = conn.channel()
    channel.open()
    channel.basic_qos(0, 5, False)
    channel.queue_declare("teste_mira", passive=False, durable=True, auto_delete=False)
    p = channel.basic_consume("teste_mira", callback=on_message)
    print(str(p))


if __name__ == "__main__":
    pub()
    while True:
        time.sleep(1)
    input("rodando")