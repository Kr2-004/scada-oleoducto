"""Simulador de campo del oleoducto.

- PS-01 (estacion de bombeo): servidor Modbus TCP en el puerto 5020,
  escrito solo con la libreria estandar para que veas la trama completa.
- TE-01 (terminal de entrega): publica JSON por MQTT cada ciclo
  (opcional; requiere paho-mqtt 2.x y un broker).
"""
import json
import random
import socketserver
import struct
import threading
import time

MODBUS_PORT = 5020
MQTT_HOST, MQTT_PORT = "localhost", 1883
MQTT_TOPIC = "oleoducto/TE-01"
ACELERACION = 60      # 1 s real = 60 s de proceso
PERIODO = 1.0         # segundos entre calculos

# ---- Mapa Modbus de PS-01 (offset base 0; direccion Modbus = offset + 1) ----
CO_CMD_P101, CO_CMD_P102 = 0, 1        # coils 1 y 2: comando de marcha
CO_FUGA, CO_FALLA_P101 = 9, 10         # coils 10 y 11: inyectar fuga / falla
DI_P101_RUN = 0                        # entradas 1-4: run P101, falla P101, run P102, falla P102
HR_LT101, HR_PT101, HR_PT102, HR_FT101 = 0, 2, 4, 6   # float32 = 2 registros

candado = threading.Lock()
coils = [0] * 100          # tabla 0x, lectura y escritura
entradas = [0] * 100       # tabla 1x, solo lectura
holding = [0] * 100        # tabla 4x
input_regs = [0] * 100     # tabla 3x


# ------------------------- Servidor Modbus TCP -------------------------
def empacar_bits(bits):
    datos = bytearray((len(bits) + 7) // 8)
    for i, bit in enumerate(bits):
        if bit:
            datos[i // 8] |= 1 << (i % 8)
    return bytes([len(datos)]) + bytes(datos)


def excepcion(funcion, codigo):
    return bytes([funcion | 0x80, codigo])


def atender_pdu(pdu):
    """Recibe la PDU (funcion + datos) y devuelve la PDU de respuesta."""
    funcion = pdu[0]
    with candado:
        if funcion in (1, 2):                       # leer coils / entradas discretas
            inicio, cantidad = struct.unpack(">HH", pdu[1:5])
            tabla = coils if funcion == 1 else entradas
            if inicio + cantidad > len(tabla):
                return excepcion(funcion, 2)        # direccion ilegal
            return bytes([funcion]) + empacar_bits(tabla[inicio:inicio + cantidad])
        if funcion in (3, 4):                       # leer holding / input registers
            inicio, cantidad = struct.unpack(">HH", pdu[1:5])
            tabla = holding if funcion == 3 else input_regs
            if inicio + cantidad > len(tabla):
                return excepcion(funcion, 2)
            valores = tabla[inicio:inicio + cantidad]
            return bytes([funcion, cantidad * 2]) + struct.pack(">%dH" % cantidad, *valores)
        if funcion == 5:                            # escribir un coil
            direccion, valor = struct.unpack(">HH", pdu[1:5])
            if direccion >= len(coils):
                return excepcion(funcion, 2)
            coils[direccion] = 1 if valor == 0xFF00 else 0
            return pdu
        if funcion == 6:                            # escribir un registro
            direccion, valor = struct.unpack(">HH", pdu[1:5])
            if direccion >= len(holding):
                return excepcion(funcion, 2)
            holding[direccion] = valor
            return pdu
        if funcion == 15:                           # escribir varios coils
            inicio, cantidad = struct.unpack(">HH", pdu[1:5])
            if inicio + cantidad > len(coils):
                return excepcion(funcion, 2)
            for i in range(cantidad):
                coils[inicio + i] = (pdu[6 + i // 8] >> (i % 8)) & 1
            return pdu[:5]
        if funcion == 16:                           # escribir varios registros
            inicio, cantidad = struct.unpack(">HH", pdu[1:5])
            if inicio + cantidad > len(holding):
                return excepcion(funcion, 2)
            holding[inicio:inicio + cantidad] = struct.unpack(">%dH" % cantidad, pdu[6:6 + cantidad * 2])
            return pdu[:5]
    return excepcion(funcion, 1)                    # funcion ilegal


def leer_exacto(conexion, n):
    datos = b""
    while len(datos) < n:
        parte = conexion.recv(n - len(datos))
        if not parte:
            return None
        datos += parte
    return datos


class ManejadorModbus(socketserver.BaseRequestHandler):
    def handle(self):
        while True:
            # Cabecera MBAP: transaccion (2), protocolo (2), longitud (2), unidad (1)
            cabecera = leer_exacto(self.request, 7)
            if cabecera is None:
                return
            transaccion, protocolo, longitud, unidad = struct.unpack(">HHHB", cabecera)
            pdu = leer_exacto(self.request, longitud - 1)
            if pdu is None:
                return
            respuesta = atender_pdu(pdu)
            self.request.sendall(
                struct.pack(">HHHB", transaccion, protocolo, len(respuesta) + 1, unidad) + respuesta
            )


class ServidorModbus(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


# --------------------------- Modelo de proceso ---------------------------
def float_a_registros(valor):
    """float32 -> 2 registros de 16 bits, palabra alta primero (orden ABCD)."""
    return list(struct.unpack(">HH", struct.pack(">f", valor)))


def ruido(valor, porcentaje=0.3):
    return valor + random.gauss(0, abs(valor) * porcentaje / 100 + 0.01)


def conectar_mqtt():
    try:
        import paho.mqtt.client as mqtt
        cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sim-te01")
        cliente.will_set(MQTT_TOPIC + "/estado", "OFFLINE", qos=1, retain=True)
        cliente.username_pw_set("admin", "password")
        cliente.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    except (ImportError, OSError) as error:
        print("MQTT no disponible (%s); sigo solo con Modbus." % error)
        return None
    cliente.loop_start()
    cliente.publish(MQTT_TOPIC + "/estado", "ONLINE", qos=1, retain=True)
    return cliente


def proceso(cliente_mqtt):
    nivel, caudal = 75.0, 0.0             # % y m3/h
    nivel_te, total_te = 20.0, 0.0        # % y m3
    while True:
        with candado:
            cmd1, cmd2 = coils[CO_CMD_P101], coils[CO_CMD_P102]
            fuga, falla1 = coils[CO_FUGA], coils[CO_FALLA_P101]

        run1 = bool(cmd1) and not falla1 and nivel > 5      # disparo por bajo nivel
        run2 = bool(cmd2) and nivel > 5
        caudal += (180.0 * (run1 + run2) - caudal) * 0.25   # rampa de primer orden

        horas = PERIODO * ACELERACION / 3600
        nivel += (150.0 - caudal) * horas / 5000 * 100      # tanque de 5000 m3, entran 150 m3/h
        nivel = max(0.0, min(100.0, nivel))

        succion = 20 + 3 * nivel                            # kPa
        descarga = succion + 9 * caudal                     # kPa
        perdida = 25.0 if fuga and caudal > 1 else 0.0      # m3/h
        llegada = max(0.0, caudal - perdida)
        presion_te = descarga * 0.25 - (80 if perdida else 0)
        total_te += llegada * horas
        nivel_te = min(100.0, nivel_te + llegada * horas / 8000 * 100)

        with candado:
            entradas[DI_P101_RUN:DI_P101_RUN + 4] = [int(run1), int(bool(falla1)), int(run2), 0]
            holding[HR_LT101:HR_LT101 + 2] = float_a_registros(ruido(nivel))
            holding[HR_PT101:HR_PT101 + 2] = float_a_registros(ruido(succion))
            holding[HR_PT102:HR_PT102 + 2] = float_a_registros(ruido(descarga))
            holding[HR_FT101:HR_FT101 + 2] = float_a_registros(ruido(caudal))

        if cliente_mqtt is not None:
            carga = {
                "FT_201": round(ruido(llegada), 2),
                "PT_201": round(ruido(presion_te), 1),
                "FQ_201": round(total_te, 1),
                "LT_201": round(nivel_te, 2),
            }
            cliente_mqtt.publish(MQTT_TOPIC, json.dumps(carga), qos=1)

        time.sleep(PERIODO)


if __name__ == "__main__":
    servidor = ServidorModbus(("0.0.0.0", MODBUS_PORT), ManejadorModbus)
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    print("PS-01 escuchando Modbus TCP en el puerto %d (Ctrl+C para salir)" % MODBUS_PORT)
    try:
        proceso(conectar_mqtt())
    except KeyboardInterrupt:
        print("Simulador detenido.")
