import socket
import threading
import json

# Configuración básica del servidor
HOST = '127.0.0.1'  # Localhost para pruebas
PORT = 5000         # Puerto de conexión
clientes_conectados = {}  # Diccionario para guardar {nombre_nodo: socket_cliente}
lock = threading.Lock()   # Lock para proteger el acceso concurrente al diccionario


def manejar_cliente(cliente_socket, direccion):
    """
    Esta función manejará la conexión individual de cada nodo (Monitor o Procesador).
    Se ejecutará en un hilo (thread) separado por cada cliente conectado.

    Responsabilidades (solo relay, sin lógica de negocio):
      1. Handshake — solicitar nombre e identificar al nodo.
      2. Enrutamiento privado — comando /w <nodo> <mensaje>.
      3. Broadcast — comando /broadcast o mensajes sin prefijo /w.
      4. Desconexión segura — limpiar diccionario y cerrar socket.
    """
    print(f"[NUEVA CONEXIÓN] {direccion} conectado.")
    nombre_nodo = None

    try:
        # ── 1. HANDSHAKE / IDENTIFICACIÓN ──────────────────────────────
        cliente_socket.send("IDENTIFY: Envía tu nombre de nodo.\n".encode('utf-8'))
        nombre_nodo = cliente_socket.recv(1024).decode('utf-8').strip()

        if not nombre_nodo:
            print(f"[ERROR] {direccion} no envió un nombre. Cerrando conexión.")
            cliente_socket.close()
            return

        with lock:
            clientes_conectados[nombre_nodo] = cliente_socket

        print(f"[REGISTRO] Nodo '{nombre_nodo}' registrado desde {direccion}.")
        cliente_socket.send(f"OK: Registrado como '{nombre_nodo}'.\n".encode('utf-8'))

        # ── BUCLE PRINCIPAL DE RECEPCIÓN ───────────────────────────────
        while True:
            mensaje_recibido = cliente_socket.recv(4096).decode('utf-8')
            if not mensaje_recibido:
                break  # El cliente cerró la conexión

            print(f"[{nombre_nodo}] Mensaje recibido: {mensaje_recibido}")

            # ── 2. ENRUTAMIENTO PRIVADO (/w <nodo> <mensaje>) ──────────
            if mensaje_recibido.startswith("/w "):
                partes = mensaje_recibido.split(" ", 2)  # ["/w", "destino", "contenido"]

                if len(partes) < 3:
                    cliente_socket.send(
                        "ERROR: Uso correcto → /w <nodo_destino> <mensaje>\n".encode('utf-8')
                    )
                    continue

                nodo_destino = partes[1]
                contenido = partes[2]

                with lock:
                    socket_destino = clientes_conectados.get(nodo_destino)

                if socket_destino:
                    try:
                        mensaje_formateado = f"[PRIVADO de {nombre_nodo}] {contenido}"
                        socket_destino.send(mensaje_formateado.encode('utf-8'))
                        print(f"[RELAY PRIVADO] {nombre_nodo} → {nodo_destino}")
                    except Exception as e:
                        print(f"[ERROR ENVÍO] No se pudo enviar a '{nodo_destino}': {e}")
                        cliente_socket.send(
                            f"ERROR: Falló el envío a '{nodo_destino}'.\n".encode('utf-8')
                        )
                else:
                    cliente_socket.send(
                        f"ERROR: Nodo '{nodo_destino}' no encontrado.\n".encode('utf-8')
                    )

            # ── 3. BROADCAST (/broadcast o cualquier otro mensaje) ─────
            else:
                # Quitar el prefijo /broadcast si existe
                if mensaje_recibido.startswith("/broadcast "):
                    contenido = mensaje_recibido[len("/broadcast "):]
                else:
                    contenido = mensaje_recibido

                mensaje_formateado = f"[BROADCAST de {nombre_nodo}] {contenido}"

                with lock:
                    destinatarios = list(clientes_conectados.items())

                for nombre, sock in destinatarios:
                    if nombre != nombre_nodo:  # No reenviar al emisor
                        try:
                            sock.send(mensaje_formateado.encode('utf-8'))
                        except Exception as e:
                            print(f"[ERROR BROADCAST] No se pudo enviar a '{nombre}': {e}")

                print(f"[RELAY BROADCAST] {nombre_nodo} → todos")

    except ConnectionResetError:
        print(f"[DESCONEXIÓN ABRUPTA] '{nombre_nodo or direccion}' cerró la conexión.")
    except Exception as e:
        print(f"[ERROR] En hilo de '{nombre_nodo or direccion}': {e}")
    finally:
        # ── 4. MANEJO DE DESCONEXIONES ─────────────────────────────────
        if nombre_nodo:
            with lock:
                clientes_conectados.pop(nombre_nodo, None)
            print(f"[LIMPIEZA] Nodo '{nombre_nodo}' eliminado del registro.")

        try:
            cliente_socket.close()
        except Exception:
            pass

        print(f"[DESCONEXIÓN] {nombre_nodo or direccion} se ha desconectado.")


def iniciar_servidor():
    """
    Inicia el servidor y escucha nuevas conexiones entrantes.
    """
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((HOST, PORT))
    servidor.listen()
    print(f"[LISTO] El servidor central está escuchando en {HOST}:{PORT}...")

    while True:
        # Aceptamos la conexión de un nuevo cliente
        cliente_socket, direccion = servidor.accept()

        # Creamos un nuevo hilo para manejar a este cliente en paralelo
        hilo = threading.Thread(target=manejar_cliente, args=(cliente_socket, direccion))
        hilo.daemon = True  # El hilo muere si el proceso principal termina
        hilo.start()
        print(f"[HILOS ACTIVOS] {threading.active_count() - 1}")


if __name__ == "__main__":
    iniciar_servidor()