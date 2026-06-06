import socket
import threading
import json

# Configuración básica del servidor
HOST = '127.0.0.1'  # Localhost para pruebas
PORT = 5000         # Puerto de conexión
clientes_conectados = {}  # Diccionario para guardar {nombre_nodo: socket_cliente}

def manejar_cliente(cliente_socket, direccion):
    """
    Esta función manejará la conexión individual de cada nodo (Monitor o Procesador).
    Se ejecutará en un hilo (thread) separado por cada cliente conectado.
    """
    print(f"[NUEVA CONEXIÓN] {direccion} conectado.")
    
    # 1. Aquí pediremos al cliente que se identifique (ej: "Monitor", "Validador1")
    # 2. Guardaremos su socket en el diccionario 'clientes_conectados'
    
    try:
        while True:
            # Esperamos recibir mensajes en formato JSON sobre TCP
            mensaje_recibido = cliente_socket.recv(1024).decode('utf-8')
            if not mensaje_recibido:
                break
                
            # Aquí implementaremos la lógica para detectar si es un mensaje de broadcast 
            # o un mensaje privado usando el comando /w [cite: 66]
            print(f"Mensaje recibido: {mensaje_recibido}")
            
    except:
        pass
    finally:
        # Lógica para manejar la desconexión de un cliente
        print(f"[DESCONEXIÓN] {direccion} se ha desconectado.")
        cliente_socket.close()

def iniciar_servidor():
    """
    Inicia el servidor y escucha nuevas conexiones entrantes.
    """
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, PORT))
    servidor.listen()
    print(f"[LISTO] El servidor central está escuchando en {HOST}:{PORT}...")

    while True:
        # Aceptamos la conexión de un nuevo cliente
        cliente_socket, direccion = servidor.accept()
        
        # Creamos un nuevo hilo para manejar a este cliente en paralelo
        hilo = threading.Thread(target=manejar_cliente, args=(cliente_socket, direccion))
        hilo.start()
        print(f"[HILOS ACTIVOS] {threading.active_count() - 1}")

if __name__ == "__main__":
    iniciar_servidor()