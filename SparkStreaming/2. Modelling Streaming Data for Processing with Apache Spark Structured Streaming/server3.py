import socket
import time
from datetime import datetime, timedelta

host, port = ('localhost', 65432)
inc = 20

print("Starting server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.bind((host, port))
	s.listen()
	conn, addr = s.accept()
	with conn:
		print('Connected by', addr)
		while True:
			inc += 2
			conn.sendall((str(datetime.now()) + "," + str(inc) + ",current\n").encode())
			conn.sendall((str(datetime.now() - timedelta(hours=1)) + "," + str(inc) + ",late\n").encode())
			conn.sendall((str(datetime.now() - timedelta(days=1)) + "," + str(inc) + ",expired\n").encode())
			time.sleep(4)
			