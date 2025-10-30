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
        """Obtiene conteos de la tabla DynamoDB"""
        try:
            # Contar archivos pendientes (ocr_done = false)
            response_false = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('false'),
                Select='COUNT'
            )
            pending = response_false['Count']
            
            # Contar archivos en proceso (ocr_done = in_process)
            response_in_process = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('in_process'),
                Select='COUNT'
            )
            in_process = response_in_process['Count']
            
            # Contar archivos completados (ocr_done = true)
            response_true = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('true'),
                Select='COUNT'
            )
            completed = response_true['Count']
            
            total = pending + in_process + completed
            
            return {
                'pending': pending,
                'in_process': in_process,
                'completed': completed,
                'total': total
            }
            
        except ClientError as e:
            print(f"Error obteniendo conteos de DynamoDB: {e}")
            return None
    
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
            errors=0,
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
                errors=0,
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
                    errors=0,
                    milestone_message=message
                )
                self.milestones_sent[milestone] = True
    
    def run_continuous_processing(self, language: str = 'spa'):
        """Ejecuta el procesamiento continuo hasta completar todos los archivos"""
        print("=== OCR Orchestrator - Procesamiento Continuo ===\n")
        
        # Obtener conteo inicial
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
        print()
        
        # Enviar email de inicio
        self.send_startup_email(initial_counts)
        
        if initial_counts['pending'] == 0 and initial_counts['in_process'] == 0:
            print("✅ No hay archivos pendientes para procesar")
            return True
        
        # Contador de procesamiento
        processed_in_session = 0
        iteration = 0
        
        print("🚀 Iniciando procesamiento continuo...\n")
        
        while True:
            iteration += 1
            print(f"\n{'='*60}")
            print(f"Iteración {iteration}")
            print(f"{'='*60}")
            
            # Procesar un archivo
            success = self.processor.process_single_pdf(language)
            
            if not success:
                print("\n⚠️ No hay más archivos para procesar o hubo un error")
                break
            
            processed_in_session += 1
            print(f"\n✅ Archivos procesados en esta sesión: {processed_in_session}")
            
            # Obtener conteos actualizados
            current_counts = self.get_table_counts()
            if current_counts:
                print(f"📊 Estado actual: {current_counts['completed']}/{current_counts['total']} completados")
                
                # Verificar y enviar emails de hitos
                self.check_and_send_milestone_email(current_counts)
            
            print(f"{'='*60}\n")
        
        # Resumen final
        print("\n" + "="*60)
        print("=== RESUMEN FINAL ===")
        print("="*60)
        
        final_counts = self.get_table_counts()
        if final_counts:
            print(f"Total procesado en esta sesión: {processed_in_session}")
            print(f"Total de archivos completados: {final_counts['completed']}/{final_counts['total']}")
            print(f"Archivos pendientes: {final_counts['pending']}")
            
            # Enviar email final si alcanzamos 100%
            if final_counts['pending'] == 0 and final_counts['in_process'] == 0:
                print("\n🎉 ¡Todos los archivos han sido procesados!")
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
