"""Cliente Modbus TCP minimo: arranca P-101 y lee PS-01.

Solo usa la libreria estandar. Cada peticion es:
cabecera MBAP (7 bytes) + codigo de funcion + datos.
"""
import socket
import struct
import time

HOST, PUERTO, UNIDAD = "localhost", 5020, 1
transaccion = 0


def pedir(conexion, pdu):
    global transaccion
    transaccion += 1
    conexion.sendall(struct.pack(">HHHB", transaccion, 0, len(pdu) + 1, UNIDAD) + pdu)
    cabecera = conexion.recv(7)
    longitud = struct.unpack(">HHHB", cabecera)[2]
    respuesta = conexion.recv(longitud - 1)
    if respuesta[0] & 0x80:
        raise RuntimeError("Excepcion Modbus, codigo %d" % respuesta[1])
    return respuesta


def leer_float(conexion, offset):
    r = pedir(conexion, struct.pack(">BHH", 3, offset, 2))      # funcion 03
    return struct.unpack(">f", r[2:6])[0]


def leer_entradas(conexion, offset, cantidad):
    r = pedir(conexion, struct.pack(">BHH", 2, offset, cantidad))  # funcion 02
    return [(r[2] >> i) & 1 for i in range(cantidad)]


with socket.create_connection((HOST, PUERTO), timeout=3) as c:
    pedir(c, struct.pack(">BHH", 5, 0, 0xFF00))                 # funcion 05: coil 1 = ON
    for _ in range(5):
        time.sleep(1)
        print(
            "run1/falla1/run2/falla2:", leer_entradas(c, 0, 4),
            "| nivel %.1f %%" % leer_float(c, 0),
            "| succion %.0f kPa" % leer_float(c, 2),
            "| descarga %.0f kPa" % leer_float(c, 4),
            "| caudal %.1f m3/h" % leer_float(c, 6),
        )
