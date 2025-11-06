#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import configparser
from full_process import FullOCRProcessor
from send_mail import SESMailer
from botocore.exceptions import ClientError

class OCROrchestrator:
    def __init__(self, config_file: str = 'config.conf'):
        """Inicializa el orquestador con todas las dependencias"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        # Inicializar componentes
        self.processor = FullOCRProcessor(config_file)
        self.mailer = SESMailer(config_file)
        
        # Configurar DynamoDB para queries
        self.region = self.config.get('AWS', 'region')
        self.table_name = self.config.get('AWS', 'dynamo_table')
        self.aws_access_key_id = self.config.get('AWS', 'aws_access_key_id')
        self.aws_secret_access_key = self.config.get('AWS', 'aws_secret_access_key')
        
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
        self.table = self.dynamodb.Table(self.table_name)
        
        # Control de hitos enviados
        self.milestones_sent = {
            100: False,
            20: False,
            40: False,
            80: False,
            100: False  # Porcentaje
        }
        
        self.initial_total = 0
        self.initial_pending = 0
    
    def get_table_counts(self):
        """Obtiene conteos de la tabla DynamoDB manejando paginación"""
        try:
            print("🔍 Escaneando tabla completa de DynamoDB (puede tomar un momento)...")
            
            # Inicializar contadores
            pending = 0
            in_process = 0
            completed = 0
            errors = 0
            
            # Escanear toda la tabla con paginación
            last_evaluated_key = None
            scan_count = 0
            
            while True:
                scan_count += 1
                print(f"   Escaneando lote {scan_count}...")
                
                # Preparar parámetros de scan
                scan_params = {
                    'ProjectionExpression': 'ocr_done',
                    'Select': 'SPECIFIC_ATTRIBUTES'
                }
                
                if last_evaluated_key:
                    scan_params['ExclusiveStartKey'] = last_evaluated_key
                
                # Ejecutar scan
                response = self.table.scan(**scan_params)
                
                # Contar elementos en este lote
                for item in response.get('Items', []):
                    ocr_status = item.get('ocr_done', 'unknown')
                    if ocr_status == 'false':
                        pending += 1
                    elif ocr_status == 'in_process':
                        in_process += 1
                    elif ocr_status == 'true':
                        completed += 1
                    elif ocr_status == 'error':
                        errors += 1
                
                # Verificar si hay más páginas
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
                
                # Mostrar progreso cada 10 lotes
                if scan_count % 10 == 0:
                    total_so_far = pending + in_process + completed + errors
                    print(f"   Progreso: {total_so_far} registros escaneados...")
            
            total = pending + in_process + completed + errors
            
            print(f"✅ Escaneo completo: {total} registros procesados en {scan_count} lotes")
            
            return {
                'pending': pending,
                'in_process': in_process,
                'completed': completed,
                'errors': errors,
                'total': total
            }
            
        except ClientError as e:
            print(f"Error obteniendo conteos de DynamoDB: {e}")
            return None

    def get_quick_pending_count(self):
        """Obtiene un conteo rápido solo de archivos pendientes"""
        try:
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('false'),
                Select='COUNT'
            )
            
            pending = response['Count']
            
            # Manejar paginación para conteo exacto
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('false'),
                    Select='COUNT',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                pending += response['Count']
            
            return pending
            
        except ClientError as e:
            print(f"Error obteniendo conteo rápido: {e}")
            return 0

    def send_startup_email(self, counts):
        """Envía email al iniciar el procesamiento"""
        if not counts:
            return
        
        progress_percent = int((counts['completed'] / counts['total'] * 100)) if counts['total'] > 0 else 0
        
        print("📧 Enviando email de inicio...")
        success = self.mailer.send_sync_progress_email(
            progress_percent=progress_percent,
            processed=counts['completed'],
            total=counts['total'],
            errors=counts['errors'],
            milestone_message=f"🚀 Proceso OCR iniciado - {counts['pending']} archivos pendientes"
        )
        
        if success:
            print("✅ Email de inicio enviado exitosamente")
        else:
            print("⚠️ No se pudo enviar email de inicio (el proceso continuará)")
    
    def check_and_send_milestone_email(self, counts):
        """Verifica si se alcanzó un hito y envía email si corresponde"""
        completed = counts['completed']
        total = counts['total']
        
        if total == 0:
            return
        
        # Calcular progreso porcentual
        progress_percent = int((completed / total) * 100)
        
        # Hito por número absoluto (100 archivos)
        if completed >= 100 and not self.milestones_sent[100]:
            print(f"\n🎯 Hito alcanzado: 100 archivos procesados!")
            self.mailer.send_sync_progress_email(
                progress_percent=progress_percent,
                processed=completed,
                total=total,
                errors=counts['errors'],
                milestone_message="¡100 archivos procesados exitosamente!"
            )
            self.milestones_sent[100] = True
        
        # Hitos por porcentaje
        milestones = [20, 40, 80, 100]
        for milestone in milestones:
            if progress_percent >= milestone and not self.milestones_sent[milestone]:
                print(f"\n🎯 Hito alcanzado: {milestone}% completado!")
                
                # Mensaje especial para cada hito
                if milestone == 100:
                    message = "¡Procesamiento completado al 100%!"
                else:
                    message = f"¡{milestone}% del procesamiento completado!"
                
                self.mailer.send_sync_progress_email(
                    progress_percent=progress_percent,
                    processed=completed,
                    total=total,
                    errors=counts['errors'],
                    milestone_message=message
                )
                self.milestones_sent[milestone] = True
    
    def run_continuous_processing(self, language: str = 'spa'):
        """Ejecuta el procesamiento continuo hasta completar todos los archivos"""
        print("=== OCR Orchestrator - Procesamiento Continuo ===\n")
        
        # Obtener conteo inicial (completo)
        print("📊 Obteniendo información inicial de DynamoDB...")
        initial_counts = self.get_table_counts()
        
        if not initial_counts:
            print("❌ Error obteniendo información de DynamoDB")
            return False
        
        self.initial_total = initial_counts['total']
        self.initial_pending = initial_counts['pending']
        
        print(f"Total de archivos en tabla: {initial_counts['total']}")
        print(f"Archivos pendientes: {initial_counts['pending']}")
        print(f"Archivos en proceso: {initial_counts['in_process']}")
        print(f"Archivos completados: {initial_counts['completed']}")
        print(f"Archivos con error: {initial_counts['errors']}")
        print()
        
        # Enviar email de inicio
        self.send_startup_email(initial_counts)
        
        if initial_counts['pending'] == 0 and initial_counts['in_process'] == 0:
            print("✅ No hay archivos pendientes para procesar")
            return True
        
        # Contador de procesamiento
        processed_in_session = 0
        error_in_session = 0
        iteration = 0
        consecutive_no_files = 0
        
        print("🚀 Iniciando procesamiento continuo...\n")
        
        while True:
            iteration += 1
            print(f"\n{'='*60}")
            print(f"Iteración {iteration}")
            print(f"{'='*60}")
            
            # Verificación rápida de archivos pendientes cada 10 iteraciones
            if iteration % 10 == 1 and iteration > 1:
                quick_pending = self.get_quick_pending_count()
                print(f"🔍 Verificación rápida: {quick_pending} archivos pendientes")
                if quick_pending == 0:
                    print("✅ No hay más archivos pendientes (verificación rápida)")
                    break
            
            # Procesar un archivo
            result = self.processor.process_single_pdf(language)
            
            if result is True:
                # Éxito
                processed_in_session += 1
                consecutive_no_files = 0
                print(f"\n✅ Archivos procesados exitosamente en esta sesión: {processed_in_session}")
            elif result is None:
                # Error, pero continuar con siguiente archivo
                error_in_session += 1
                consecutive_no_files = 0
                print(f"\n⚠️ Error procesando archivo (continuando con el siguiente)")
                print(f"   Errores en esta sesión: {error_in_session}")
            else:  # result is False
                # No hay archivos disponibles
                consecutive_no_files += 1
                if consecutive_no_files >= 3:  # Aumentar a 3 intentos
                    print("\n✅ No hay más archivos disponibles para procesar")
                    break
                else:
                    print(f"\n⚠️ No se encontraron archivos (intento {consecutive_no_files}/3)")
                    continue
            
            # Obtener conteos actualizados cada 5 iteraciones exitosas
            if processed_in_session % 5 == 0 or error_in_session % 5 == 0:
                print("📊 Actualizando estadísticas...")
                current_counts = self.get_table_counts()
                if current_counts:
                    print(f"📊 Estado actual: {current_counts['completed']}/{current_counts['total']} completados")
                    if current_counts['errors'] > 0:
                        print(f"⚠️ Archivos con error: {current_counts['errors']}")
                    if current_counts['pending'] > 0:
                        print(f"📋 Archivos pendientes: {current_counts['pending']}")
                    
                    # Verificar y enviar emails de hitos
                    self.check_and_send_milestone_email(current_counts)
            
            print(f"{'='*60}\n")
        
        # Resumen final (completo)
        print("\n" + "="*60)
        print("=== RESUMEN FINAL ===")
        print("="*60)
        
        print("📊 Obteniendo estadísticas finales...")
        final_counts = self.get_table_counts()
        if final_counts:
            print(f"Total procesado exitosamente en esta sesión: {processed_in_session}")
            print(f"Total de errores en esta sesión: {error_in_session}")
            print(f"Total de archivos completados: {final_counts['completed']}/{final_counts['total']}")
            print(f"Archivos pendientes: {final_counts['pending']}")
            print(f"Archivos con error: {final_counts['errors']}")
            
            # Enviar email final si alcanzamos 100%
            if final_counts['pending'] == 0 and final_counts['in_process'] == 0:
                print("\n🎉 ¡Todos los archivos disponibles han sido procesados!")
                if final_counts['errors'] > 0:
                    print(f"⚠️ {final_counts['errors']} archivos tuvieron errores y no se reintentarán")
                if not self.milestones_sent[100]:
                    self.check_and_send_milestone_email(final_counts)
        
        print("="*60 + "\n")
        
        return True


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Orquestador de procesamiento OCR continuo con notificaciones'
    )
    parser.add_argument(
        '--language', '-l',
        default='spa',
        help='Idioma para OCR (default: spa)'
    )
    parser.add_argument(
        '--config',
        default='config.conf',
        help='Archivo de configuración (default: config.conf)'
    )
    
    args = parser.parse_args()
    
    try:
        orchestrator = OCROrchestrator(args.config)
        success = orchestrator.run_continuous_processing(args.language)
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario")
        print("Los archivos en proceso quedarán marcados como 'in_process'")
        print("Ejecuta el script nuevamente para continuar")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
