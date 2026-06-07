import socket
import threading
import json
import time
import argparse


HOST = "127.0.0.1"
PORT = 5000

NOMBRE_MONITOR = "Monitor"
ARCHIVO_BLOQUES = "bloques.txt"

VALIDADORES_DEFAULT = ["Validador1", "Validador2", "Validador3"]


class Monitor:
    def __init__(self, host, port, validadores, dificultad):
        self.host = host
        self.port = port
        self.validadores = validadores
        self.dificultad = dificultad

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.bloques = []
        self.ledger = []

        self.bloque_actual = None
        self.votos_positivos = {}
        self.votos_negativos = {}

        self.lock = threading.Lock()
        self.consenso_event = threading.Event()

    def conectar(self):
        self.sock.connect((self.host, self.port))

        # El servidor exige que el cliente se identifique al conectarse
        self.sock.sendall(NOMBRE_MONITOR.encode("utf-8"))

        print(f"[MONITOR] Conectado al servidor como {NOMBRE_MONITOR}")

        hilo_escucha = threading.Thread(target=self.escuchar_chat, daemon=True)
        hilo_escucha.start()

    def cargar_bloques(self):
        try:
            with open(ARCHIVO_BLOQUES, "r", encoding="utf-8") as archivo:
                for linea in archivo:
                    linea = linea.strip()

                    if not linea:
                        continue

                    bloque = json.loads(linea)
                    self.bloques.append(bloque)

            print(f"[MONITOR] Se cargaron {len(self.bloques)} bloques desde {ARCHIVO_BLOQUES}")

        except FileNotFoundError:
            print(f"[ERROR] No se encontró el archivo {ARCHIVO_BLOQUES}")
            exit()

        except json.JSONDecodeError as e:
            print(f"[ERROR] El archivo {ARCHIVO_BLOQUES} tiene JSON inválido.")
            print(e)
            exit()

    def enviar_mensaje(self, mensaje):
        self.sock.sendall(mensaje.encode("utf-8"))

    def enviar_bloque_a_validadores(self, bloque):
        bloque_json = json.dumps(bloque)

        print("\n" + "=" * 60)
        print(f"[MONITOR] Enviando bloque candidato ID-{bloque.get('id')}")
        print("=" * 60)

        for validador in self.validadores:
            mensaje = f"/w {validador} {bloque_json}"
            self.enviar_mensaje(mensaje)
            print(f"[MONITOR] Bloque ID-{bloque.get('id')} enviado a {validador}")
            time.sleep(0.3)

    def escuchar_chat(self):
        while True:
            try:
                mensaje = self.sock.recv(4096).decode("utf-8")

                if not mensaje:
                    break

                print(f"[CHAT] {mensaje}")

                self.procesar_voto(mensaje)

            except ConnectionResetError:
                print("[MONITOR] Se perdió la conexión con el servidor.")
                break

            except Exception as e:
                print(f"[ERROR ESCUCHA MONITOR] {e}")
                break

    def extraer_nombre_validador(self, mensaje):
        """
        Esperamos mensajes tipo:
        [BROADCAST de Validador1] BLOQUE_OK 001 abc123...
        [BROADCAST de Validador2] BLOQUE_INVALIDO 001
        """

        if "[BROADCAST de " not in mensaje:
            return None

        try:
            inicio = mensaje.index("[BROADCAST de ") + len("[BROADCAST de ")
            fin = mensaje.index("]", inicio)
            return mensaje[inicio:fin].strip()
        except ValueError:
            return None

    def procesar_voto(self, mensaje):
        if "BLOQUE_OK" not in mensaje and "BLOQUE_INVALIDO" not in mensaje:
            return

        validador = self.extraer_nombre_validador(mensaje)

        if validador is None:
            return

        partes = mensaje.split()

        if "BLOQUE_OK" in partes:
            indice = partes.index("BLOQUE_OK")

            if len(partes) < indice + 3:
                return

            bloque_id = partes[indice + 1]
            hash_bloque = partes[indice + 2]

            self.registrar_voto_positivo(validador, bloque_id, hash_bloque)

        elif "BLOQUE_INVALIDO" in partes:
            indice = partes.index("BLOQUE_INVALIDO")

            if len(partes) < indice + 2:
                return

            bloque_id = partes[indice + 1]

            self.registrar_voto_negativo(validador, bloque_id)

    def registrar_voto_positivo(self, validador, bloque_id, hash_bloque):
        with self.lock:
            if self.bloque_actual is None:
                return

            if str(self.bloque_actual.get("id")) != str(bloque_id):
                return

            if validador in self.votos_positivos or validador in self.votos_negativos:
                print(f"[MONITOR] Voto duplicado ignorado de {validador}")
                return

            self.votos_positivos[validador] = hash_bloque

            print(f"[MONITOR] Voto positivo recibido de {validador}")
            print(f"[MONITOR] Total positivos: {len(self.votos_positivos)}")

            self.verificar_quorum()

    def registrar_voto_negativo(self, validador, bloque_id):
        with self.lock:
            if self.bloque_actual is None:
                return

            if str(self.bloque_actual.get("id")) != str(bloque_id):
                return

            if validador in self.votos_positivos or validador in self.votos_negativos:
                print(f"[MONITOR] Voto duplicado ignorado de {validador}")
                return

            self.votos_negativos[validador] = True

            print(f"[MONITOR] Voto negativo recibido de {validador}")
            print(f"[MONITOR] Total negativos: {len(self.votos_negativos)}")

            self.verificar_quorum()

    def verificar_quorum(self):
        total_validadores = len(self.validadores)
        mayoria_simple = (total_validadores // 2) + 1

        if len(self.votos_positivos) >= mayoria_simple:
            print("\n[MONITOR] MAYORÍA SIMPLE ALCANZADA")
            self.consenso_event.set()

    def procesar_bloques(self):
        for bloque in self.bloques:
            with self.lock:
                self.bloque_actual = bloque
                self.votos_positivos = {}
                self.votos_negativos = {}
                self.consenso_event.clear()

            self.enviar_bloque_a_validadores(bloque)

            print(f"[MONITOR] Esperando votos para bloque ID-{bloque.get('id')}...")

            consenso = self.consenso_event.wait(timeout=30)

            if consenso:
                with self.lock:
                    self.ledger.append({
                        "bloque": self.bloque_actual,
                        "votos_positivos": self.votos_positivos.copy(),
                        "votos_negativos": self.votos_negativos.copy()
                    })

                print(f"[MONITOR] Bloque ID-{bloque.get('id')} insertado en el Ledger local.")

                mensaje_consenso = f"/broadcast CONSENSO_ALCANZADO Bloque ID-{bloque.get('id')}"
                self.enviar_mensaje(mensaje_consenso)

            else:
                print(f"[MONITOR] No se alcanzó consenso para el bloque ID-{bloque.get('id')}")
                mensaje_fallo = f"/broadcast CONSENSO_FALLIDO Bloque ID-{bloque.get('id')}"
                self.enviar_mensaje(mensaje_fallo)

            time.sleep(2)

        self.mostrar_ledger()

    def mostrar_ledger(self):
        print("\n" + "#" * 60)
        print("[MONITOR] ESTADO GLOBAL / LEDGER LOCAL")
        print("#" * 60)

        if not self.ledger:
            print("[MONITOR] El ledger está vacío.")
            return

        for entrada in self.ledger:
            bloque = entrada["bloque"]
            print(f"Bloque ID-{bloque.get('id')}")
            print(f"Data: {bloque.get('data')}")
            print(f"Prev Hash: {bloque.get('prev_hash')}")
            print(f"Votos positivos: {list(entrada['votos_positivos'].keys())}")
            print("-" * 60)


def main():
    parser = argparse.ArgumentParser(description="Monitor / Orquestador del sistema de consenso")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--dificultad", default="0")
    parser.add_argument(
        "--validadores",
        nargs="+",
        default=VALIDADORES_DEFAULT,
        help="Lista de validadores. Ejemplo: --validadores Validador1 Validador2 Validador3"
    )

    args = parser.parse_args()

    monitor = Monitor(
        host=args.host,
        port=args.port,
        validadores=args.validadores,
        dificultad=args.dificultad
    )

    monitor.conectar()
    monitor.cargar_bloques()

    time.sleep(1)

    monitor.procesar_bloques()


if __name__ == "__main__":
    main()