# Import libraries
import socket
import threading
import os
import json
import time


# Declaring constants
TRANSMIT_SIZE = 64
REQ_API = 16
CONNECT_ID = 16
TOPIC_SIZE = 32
MSG_SIZE = 64
FLAG = 16
SEPERATOR = "~^*"

FORMAT = "utf-8"  # Decoding format
HEADER = 64  # Header length
PORT = 5050  # Socket port
# Server IP address (local)
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
DISCONNECT_MESSAGE = "!DISCONNECT"  # Message to disconnect connection

# creating directory store
cur = os.getcwd()
cur_list = os.listdir(cur)
print(cur_list)
if 'main' not in cur_list:
    os.mkdir('main')
    cur_list = os.listdir(cur)

# print(cur_list)
# path = os.path.join(cur, 'main')
# print(os.listdir(path))
# print(path)
# Create socket
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)


def producer_handler(conn, addr):
    connect_id = conn.recv(CONNECT_ID).decode(FORMAT)
    connect_id = int(connect_id)

    topic_size = conn.recv(TOPIC_SIZE).decode(FORMAT)
    topic_size = int(topic_size)

    topic = conn.recv(topic_size).decode(FORMAT)

    if topic == DISCONNECT_MESSAGE:
        print(f"{addr} is Disconnecting")
        #print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

    msg_size = conn.recv(MSG_SIZE).decode(FORMAT)
    msg_size = int(msg_size)

    message = conn.recv(msg_size).decode(FORMAT)

    path = os.path.join(cur, 'main')
    main_list = os.listdir(path)

    if topic != DISCONNECT_MESSAGE:
        if topic in main_list:
            path1 = os.path.join(path, topic)
            os.chdir(path1)
            with open('data.json', 'r') as fp:
                dictObj = json.load(fp)
            fp.close()
            dictObj.update({time.time(): message})

            with open('data.json', 'w') as json_file:
                json.dump(dictObj, json_file)
            json_file.close()

        else:
            os.chdir(path)
            os.mkdir(topic)
            path1 = os.path.join(path, topic)
            os.chdir(path1)
            dictObj = {}
            dictObj.update({time.time(): message})
            with open('data.json', 'w') as json_file:
                json.dump(dictObj, json_file)
            json_file.close()
    conn.send(str(connect_id).encode(FORMAT))

def consumer_send(index, msg, conn):

    index = index.encode(FORMAT)
    index = index + b' ' * (CONNECT_ID - len(index))

    msg = msg.encode(FORMAT)

    msg_length = len(msg)
    msg_size = str(msg_length).encode(FORMAT)
    msg_size = msg_size + b' ' * (MSG_SIZE - len(msg_size))

    conn.send(index)
    conn.send(msg_size)
    conn.send(msg)

def consumer_handler(conn, addr):
    connect_id = conn.recv(CONNECT_ID).decode(FORMAT)
    connect_id = int(connect_id)

    flag = conn.recv(FLAG).decode(FORMAT)
    flag = int(flag)

    topic_size = conn.recv(TOPIC_SIZE).decode(FORMAT)
    topic_size = int(topic_size)

    topic = conn.recv(topic_size).decode(FORMAT)

    path = os.path.join(cur, 'main')
    main_list = os.listdir(path)

    if topic in main_list:
        path1 = os.path.join(path, topic)
        os.chdir(path1)
        with open('data.json', 'r') as fp:
            dictobj = json.load(fp)
        fp.close()

        values = list(dictobj.values())
        index = len(values)  
        msg = ''
        if flag != 1:
            for instance in values[:-1]:
                msg = msg + str(instance) + SEPERATOR
            msg = msg + values[-1]
    else:
        os.chdir(path)
        os.mkdir(topic)
        path1 = os.path.join(path, topic)
        os.chdir(path1)

        index = 0
        msg = ''

    index = connect_id + index
    consumer_send(str(index), msg, conn)

    while True:
        time.sleep(1)
        with open('data.json', 'r') as fp:
            dictobj = json.load(fp)
        fp.close()
        
        values = list(dictobj.values())
        if len(values) > index: #if something was added
            msg = ''
            for instance in values[index:-1]:
                if instance in values:
                    msg = msg + str(instance) + SEPERATOR
            msg = msg + values[-1]
            consumer_send(str(index), msg, conn)
            index = len(values)
        #send message
# Keep reading messages sent from client


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")

    connected = True
    while connected:
        try:
            req_api = conn.recv(REQ_API).decode(FORMAT) 
        except:
            print(f"The client {addr} Disconnected")
            break
        if int(req_api) == 1:
            producer_handler(conn, addr)
        elif int(req_api) == 2:
            consumer_handler(conn, addr)

        # msg_length = conn.recv(HEADER).decode(FORMAT)
        # if msg_length:
        #   msg_length = int(msg_length)
        #   msg = conn.recv(msg_length).decode(FORMAT)
        #   if msg == DISCONNECT_MESSAGE:
        #     connected = False

        #   print(f"[{addr}]: {msg}")

    conn.close()


# Start listening and create new thread for each new connection
def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


print("[STARTING] Server is starting ...")
start()
