import socket
import time
from datetime import datetime

host, port = ('localhost', 65432)
inc = 120
dec = 120

print("Starting server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.bind((host, port))
	s.listen()
	conn, addr = s.accept()
	with conn:
		print('Connected by', addr)
		while True:
			inc += 2
			conn.sendall((str(datetime.now()) + "," + str(inc) + ",increasing\n").encode())

			dec -= 2
			conn.sendall((str(datetime.now()) + "," + str(dec) + ",decreasing\n").encode())

			time.sleep(4)