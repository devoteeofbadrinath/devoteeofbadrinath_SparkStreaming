import socket
import time
from datetime import datetime

host, port = ('127.0.0.1', 65432)

print("Starting server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.bind((host, port))
	s.listen()
	conn, addr = s.accept()
	with conn:
		print('Connected by', addr)
		while True:
			#conn.sendall(("1,120,ABCD123\n").encode())
			conn.sendall((str(datetime.now()) + ",120,ABCD123\n").encode())
			conn.sendall((str(datetime.now()) + ",-3,ABCD123\n").encode())
			time.sleep(4)

			