import socket
import threading
import sys

HOST = '127.0.0.1'
PORT = 5000

def recibir_mensajes(sock):
    """Hilo que escucha mensajes del servidor continuamente."""
    try:
        while True:
            mensaje = sock.recv(4096).decode('utf-8')
            if not mensaje:
                break
            print(f"\n← {mensaje}")
    except:
        pass
    finally:
        print("[INFO] Conexión con el servidor cerrada.")

def main():
    nombre = input("Tu nombre de nodo (ej. Monitor, Validador1): ").strip()
    if not nombre:
        print("Debes ingresar un nombre.")
        return

    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cliente.connect((HOST, PORT))

    # ── Handshake: recibir solicitud IDENTIFY y enviar nombre ──
    identificacion = cliente.recv(1024).decode('utf-8')
    print(f"← {identificacion}")
    cliente.send(nombre.encode('utf-8'))

    # Recibir confirmación
    confirmacion = cliente.recv(1024).decode('utf-8')
    print(f"← {confirmacion}")

    # ── Hilo para recibir mensajes del servidor ──
    hilo_receptor = threading.Thread(target=recibir_mensajes, args=(cliente,), daemon=True)
    hilo_receptor.start()

    # ── Bucle de envío ──
    print("\nComandos disponibles:")
    print("  /w <nodo> <mensaje>   → mensaje privado")
    print("  /broadcast <mensaje>  → difusión a todos")
    print("  <texto>               → difusión automática")
    print("  /salir                → desconectar\n")

    try:
        while True:
            mensaje = input("")
            if mensaje.strip().lower() == "/salir":
                break
            cliente.send(mensaje.encode('utf-8'))
    except KeyboardInterrupt:
        pass
    finally:
        cliente.close()
        print("Desconectado.")

if __name__ == "__main__":
    main()