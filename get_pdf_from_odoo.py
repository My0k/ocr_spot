# -*- coding: utf-8 -*-
"""
Script para descargar PDFs de gesdocs vía XML-RPC.

Estructura destino:
    /gesdocs/<comuna>/<tipologia>/<rbd>/<rut>/<tipo_documento>.pdf
"""

import os
import base64
import xmlrpc.client
import sys
import configparser

# === CARGAR CONFIGURACIÓN ===
config = configparser.ConfigParser()
config.read('config.conf')

# Obtener configuración Odoo
ODOO_URL = config.get('ODOO', 'url')
ODOO_DB = config.get('ODOO', 'database')
ODOO_USER = config.get('ODOO', 'username')
ODOO_PASSWORD = config.get('ODOO', 'password')

CARPETA_BASE = "/home/myok/Desktop/proyectos/ocr_spot/gesdocs"

# === CONEXIÓN XML-RPC ===
print("Conectando a Odoo...")
print(f"URL: {ODOO_URL}")
print(f"Base de datos: {ODOO_DB}")
print(f"Usuario: {ODOO_USER}")

common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})

if not uid:
    sys.exit("❌ Error de autenticación en Odoo (revisa usuario/clave/base de datos).")

models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

# === DETECTAR CAMPO BINARIO ===
print("Detectando campo binario en gesdocs.document...")

fields = models.execute_kw(
    ODOO_DB, uid, ODOO_PASSWORD,
    'gesdocs.document', 'fields_get',
    [], {'attributes': ['string', 'type']}
)

binary_fields = [f for f, info in fields.items() if info.get('type') == 'binary']
if not binary_fields:
    sys.exit("❌ No se encontraron campos binarios en gesdocs.document.")

# Se usa el primero por defecto (si hay más, puedes ajustarlo)
BINARY_FIELD = binary_fields[0]
print(f"✅ Campo binario detectado: {BINARY_FIELD}\n")

# === OBTENER DOCUMENTOS ===
document_ids = models.execute_kw(
    ODOO_DB, uid, ODOO_PASSWORD,
    'gesdocs.document', 'search',
    [[]],  # todos los documentos
    {'order': 'id asc'}
)

print(f"Se encontraron {len(document_ids)} documentos.\n")

# === PROCESAR UNO POR UNO ===
for index, doc_id in enumerate(document_ids, start=1):
    try:
        doc_data = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            'gesdocs.document', 'read',
            [doc_id],
            {'fields': ['employee_id', 'tipo_documento', BINARY_FIELD, 'name']}
        )[0]
    except Exception as e:
        print(f"⚠️ Error leyendo documento {doc_id}: {e}")
        continue

    emp_id = doc_data.get('employee_id') and doc_data['employee_id'][0] or False
    tipo_doc = doc_data.get('tipo_documento') or 'Desconocido'
    nombre_doc = doc_data.get('name') or f'doc_{doc_id}.pdf'

    if not emp_id:
        print(f"Documento {doc_id} sin empleado asociado, saltando.")
        continue

    try:
        emp_data = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            'hr.employee', 'read',
            [emp_id],
            {'fields': ['comuna_id', 'job_id', 'address_id', 'identification_id']}
        )[0]
    except Exception as e:
        print(f"⚠️ Error leyendo empleado {emp_id}: {e}")
        continue

    comuna = emp_data.get('comuna_id') and emp_data['comuna_id'][1] or 'SinComuna'
    tipologia = emp_data.get('job_id') and emp_data['job_id'][1] or 'SinCargo'
    rbd = emp_data.get('address_id') and emp_data['address_id'][1] or 'SinRBD'
    rut = emp_data.get('identification_id') or 'SinRUT'

    # Crear estructura de carpetas
    carpeta_final = os.path.join(CARPETA_BASE, comuna, tipologia, rbd, rut)
    os.makedirs(carpeta_final, exist_ok=True)

    ruta_pdf = os.path.join(carpeta_final, f"{tipo_doc}.pdf")

    # Guardar PDF
    if doc_data.get(BINARY_FIELD):
        try:
            pdf_data = base64.b64decode(doc_data[BINARY_FIELD])
            with open(ruta_pdf, "wb") as f:
                f.write(pdf_data)
            print(f"[{index}/{len(document_ids)}] ✅ Guardado: {ruta_pdf}")
        except Exception as e:
            print(f"⚠️ Error guardando {ruta_pdf}: {e}")
    else:
        print(f"[{index}/{len(document_ids)}] ⚠️ Documento sin archivo adjunto.")

    input("Presiona ENTER para continuar con el siguiente...")

print("\n🎉 Descarga completada correctamente.")
