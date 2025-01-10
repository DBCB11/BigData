import socket

host = "spark-master"
port = 7077

try:
    socket.create_connection((host, port))
    print(f"Connection to {host} was successful!")
except OSError as e:
    print(f"Connection to {host} failed: {e}")