import socket

# Nos conectamos a la IP y puerto de tu servidor
cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cliente.connect(('127.0.0.1', 5000))

# Enviamos un mensaje de texto simple codificado
mensaje = "¡Hola! Esta es una prueba de conexion exitosa."
cliente.send(mensaje.encode('utf-8'))

# Cerramos la conexión
cliente.close()