import socket
import threading
import json
import hashlib
import argparse


HOST = "127.0.0.1"
PORT = 5000


class Procesador:
    def __init__(self, nombre, host, port, dificultad):
        self.nombre = nombre
        self.host = host
        self.port = port
        self.dificultad = dificultad

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def conectar(self):
        self.sock.connect((self.host, self.port))

        # El servidor exige que el cliente se identifique al conectarse
        self.sock.sendall(self.nombre.encode("utf-8"))

        print(f"[{self.nombre}] Conectado al servidor.")
        print(f"[{self.nombre}] Esperando bloques candidatos del Monitor...")

    def enviar_mensaje(self, mensaje):
        self.sock.sendall(mensaje.encode("utf-8"))

    def escuchar(self):
        while True:
            try:
                mensaje = self.sock.recv(4096).decode("utf-8")

                if not mensaje:
                    break

                print(f"[CHAT] {mensaje}")

                if self.es_bloque_privado_del_monitor(mensaje):
                    bloque = self.extraer_bloque_json(mensaje)

                    if bloque is not None:
                        hilo = threading.Thread(
                            target=self.validar_bloque,
                            args=(bloque,),
                            daemon=True
                        )
                        hilo.start()

            except ConnectionResetError:
                print(f"[{self.nombre}] Se perdió la conexión con el servidor.")
                break

            except Exception as e:
                print(f"[ERROR {self.nombre}] {e}")
                break

    def es_bloque_privado_del_monitor(self, mensaje):
        """
        Esperamos mensajes tipo:
        [PRIVADO de Monitor] {"id": "001", "data": "...", "prev_hash": "...", "nonce": 0}
        """

        return "[PRIVADO de Monitor]" in mensaje and "{" in mensaje and "}" in mensaje

    def extraer_bloque_json(self, mensaje):
        try:
            inicio = mensaje.index("{")
            fin = mensaje.rindex("}") + 1

            bloque_json = mensaje[inicio:fin]
            bloque = json.loads(bloque_json)

            return bloque

        except json.JSONDecodeError:
            print(f"[{self.nombre}] Error: el bloque recibido no es JSON válido.")
            return None

        except ValueError:
            print(f"[{self.nombre}] Error: no se pudo extraer JSON del mensaje.")
            return None

    def calcular_hash(self, bloque):
        """
        Calcula el hash SHA-256 del bloque.
        Usamos sort_keys=True para que todos los validadores calculen exactamente el mismo hash.
        """

        bloque_string = json.dumps(bloque, sort_keys=True)
        return hashlib.sha256(bloque_string.encode("utf-8")).hexdigest()

    def verificar_hash(self, bloque):
        """
        Prueba diferentes nonce hasta que el hash comience con la dificultad indicada.

        Ejemplo:
        dificultad = "0"  -> hash debe empezar con 0
        dificultad = "00" -> hash debe empezar con 00
        """

        nonce = 0

        while True:
            bloque_modificado = bloque.copy()
            bloque_modificado["nonce"] = nonce

            hash_resultante = self.calcular_hash(bloque_modificado)

            if hash_resultante.startswith(self.dificultad):
                return bloque_modificado, hash_resultante

            nonce += 1

    def validar_bloque(self, bloque):
        bloque_id = bloque.get("id", "SIN_ID")

        print("\n" + "=" * 60)
        print(f"[{self.nombre}] Bloque candidato recibido")
        print(f"[{self.nombre}] ID: {bloque_id}")
        print(f"[{self.nombre}] Iniciando verificación hash con dificultad '{self.dificultad}'")
        print("=" * 60)

        try:
            bloque_validado, hash_valido = self.verificar_hash(bloque)

            print(f"[{self.nombre}] Bloque ID-{bloque_id} validado correctamente.")
            print(f"[{self.nombre}] Nonce encontrado: {bloque_validado['nonce']}")
            print(f"[{self.nombre}] Hash válido: {hash_valido}")

            voto = f"/broadcast BLOQUE_OK {bloque_id} {hash_valido}"
            self.enviar_mensaje(voto)

        except Exception as e:
            print(f"[{self.nombre}] Error validando bloque ID-{bloque_id}: {e}")

            voto = f"/broadcast BLOQUE_INVALIDO {bloque_id}"
            self.enviar_mensaje(voto)


def main():
    parser = argparse.ArgumentParser(description="Procesador / Validador del sistema de consenso")
    parser.add_argument("nombre", help="Nombre del validador. Ejemplo: Validador1")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--dificultad", default="0")

    args = parser.parse_args()

    procesador = Procesador(
        nombre=args.nombre,
        host=args.host,
        port=args.port,
        dificultad=args.dificultad
    )

    procesador.conectar()
    procesador.escuchar()


if __name__ == "__main__":
    main()