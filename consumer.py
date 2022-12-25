import socket

FORMAT = "utf-8"
PORT = 5050
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)

FLAG = 16
REQ_API = 16
CONNECT_ID = 16
TOPIC_SIZE = 32
MSG_SIZE = 64
ERROR_SIZE = 16
SEPERATOR = "~^*"

seq_no = 0

consumer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
consumer.connect(ADDR)

def send(topic, flag):
  topic = topic.encode(FORMAT)

  flag = flag.encode(FORMAT)
  flag = flag + b' ' * (FLAG - len(flag))

  topic_lenght = len(topic)
  topic_size = str(topic_lenght).encode(FORMAT)
  topic_size = topic_size + b' ' * (TOPIC_SIZE - len(topic_size))

  connect_id = str(seq_no).encode(FORMAT)
  connect_id = connect_id + b' ' * (CONNECT_ID - len(connect_id))

  req_api = ("2").encode(FORMAT)
  req_api = req_api + b' ' * (REQ_API - len(req_api))

  consumer.send(req_api)
  consumer.send(connect_id)
  consumer.send(flag)
  consumer.send(topic_size)
  consumer.send(topic)

topic = input("Enter topic: ")
flag = input("0 to get data from beginning, 1 for live data: ")
# Implement consumer termination
send(topic, flag)

while True:
  
  connect_id = consumer.recv(CONNECT_ID).decode(FORMAT)
  connect_id = int(connect_id)

  index = connect_id

  msg_size = consumer.recv(MSG_SIZE).decode(FORMAT)
  msg_size = int(msg_size)
  message = consumer.recv(msg_size).decode(FORMAT)
  
  print(message.split(SEPERATOR))
