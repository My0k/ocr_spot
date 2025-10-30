#!/usr/bin/env python3
"""
Script para ejecutar sincronización Odoo → S3 en modo servidor
Diseñado para ejecutarse con nohup en segundo plano

Uso:
    nohup python3 odoo_to_s3_nohup.py > sync_odoo_s3.log 2>&1 &
"""

import os
import sys
import signal
import time
from datetime import datetime
from main_tools import OCRSpotManager

class OdooS3SyncDaemon:
    def __init__(self):
        self.manager = None
        self.running = True
        self.start_time = datetime.now()
        
        # Configurar manejo de señales para parada limpia
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Maneja señales de terminación para parada limpia"""
        print(f"\n🛑 Señal {signum} recibida. Iniciando parada limpia...")
        self.running = False
    
    def log_with_timestamp(self, message):
        """Imprime mensaje con timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        sys.stdout.flush()  # Forzar escritura inmediata al log
    
    def run_sync(self):
        """Ejecuta la sincronización completa"""
        try:
            self.log_with_timestamp("🚀 Iniciando sincronización Odoo → S3 (modo servidor)")
            self.log_with_timestamp(f"📁 PID del proceso: {os.getpid()}")
            self.log_with_timestamp(f"📂 Directorio de trabajo: {os.getcwd()}")
            
            # Inicializar manager
            self.manager = OCRSpotManager()
            
            self.log_with_timestamp("✅ Configuración cargada exitosamente")
            self.log_with_timestamp(f"📦 Bucket input: {self.manager.bucket_name}")
            self.log_with_timestamp(f"📦 Bucket output: {self.manager.output_bucket}")
            self.log_with_timestamp(f"🗄️ Tabla DynamoDB: {self.manager.table_name}")
            
            # Ejecutar sincronización en modo continuo (sin pausas)
            self.log_with_timestamp("🔄 Iniciando sincronización continua...")
            self.manager.sync_odoo_to_s3_and_dynamodb(step_by_step=False)
            
            self.log_with_timestamp("✅ Sincronización completada exitosamente")
            
        except KeyboardInterrupt:
            self.log_with_timestamp("⚠️ Interrupción por teclado recibida")
        except Exception as e:
            self.log_with_timestamp(f"❌ Error durante la sincronización: {e}")
            import traceback
            self.log_with_timestamp(f"📋 Traceback completo:\n{traceback.format_exc()}")
            return False
        
        return True
    
    def run(self):
        """Función principal del daemon"""
        self.log_with_timestamp("=" * 60)
        self.log_with_timestamp("🤖 DAEMON SINCRONIZACIÓN ODOO → S3 INICIADO")
        self.log_with_timestamp("=" * 60)
        
        try:
            success = self.run_sync()
            
            # Calcular tiempo total
            end_time = datetime.now()
            duration = end_time - self.start_time
            
            self.log_with_timestamp("=" * 60)
            if success:
                self.log_with_timestamp("🎉 DAEMON FINALIZADO EXITOSAMENTE")
            else:
                self.log_with_timestamp("❌ DAEMON FINALIZADO CON ERRORES")
            
            self.log_with_timestamp(f"⏱️ Tiempo total de ejecución: {duration}")
            self.log_with_timestamp(f"🕐 Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.log_with_timestamp(f"🕐 Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.log_with_timestamp("=" * 60)
            
            return 0 if success else 1
            
        except Exception as e:
            self.log_with_timestamp(f"❌ Error fatal en daemon: {e}")
            return 1


def main():
    """Función principal"""
    # Verificar que el archivo de configuración existe
    if not os.path.exists('config.conf'):
        print("❌ Error: No se encontró el archivo config.conf")
        print("   Asegúrate de ejecutar este script desde el directorio correcto")
        return 1
    
    # Crear y ejecutar daemon
    daemon = OdooS3SyncDaemon()
    return daemon.run()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
