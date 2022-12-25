# For producer

# Header format definition

import socket

# Declaring constants
TRANSMIT_SIZE = 64
REQ_API = 16
CONNECT_ID = 16
TOPIC_SIZE = 32
MSG_SIZE = 64

FORMAT = "utf-8"
PORT = 5050
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)

seq_no = 1

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)


def send(topic, msg):
    # Encode from inside out
    message = msg.encode(FORMAT)
    msg_length = len(message)
    msg_size = str(msg_length).encode(FORMAT)
    msg_size = msg_size + b' ' * (MSG_SIZE - len(msg_size))

    topic = topic.encode(FORMAT)
    topic_lenght = len(topic)
    topic_size = str(topic_lenght).encode(FORMAT)
    topic_size = topic_size + b' ' * (TOPIC_SIZE - len(topic_size))

    connect_id = str(seq_no).encode(FORMAT)
    connect_id = connect_id + b' ' * (CONNECT_ID - len(connect_id))

    req_api = ("1").encode(FORMAT)
    req_api = req_api + b' ' * (REQ_API - len(req_api))

    client.send(req_api)
    client.send(connect_id)
    client.send(topic_size)
    client.send(topic)
    client.send(msg_size)
    client.send(message)


while True:
    message = input("Enter message: ")
    if message == DISCONNECT_MESSAGE:
        topic = DISCONNECT_MESSAGE
        send(topic, message)
        client.close()
        print("Disconnected succesfully")
        break
    else:
        topic = input("Enter topic: ")
        send(topic, message)

    ack = client.recv(1024).decode(FORMAT)
    """timer start 5 seconds flag=False...if not success flag is false itself else true resend after timer"""
    if int(ack) == seq_no:
        print("Acknowledgement recieved:", int(ack))
        seq_no += 1

    else:
        print("Resending msg")
        send(topic, message)
