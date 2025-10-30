import boto3
import configparser
from botocore.exceptions import ClientError
from datetime import datetime

class SESMailer:
    def __init__(self, config_file: str = 'config.conf'):
        """Inicializa el mailer con configuración desde archivo"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        # Obtener configuración AWS
        self.region = self.config.get('AWS', 'region')
        self.aws_access_key_id = self.config.get('AWS', 'aws_access_key_id')
        self.aws_secret_access_key = self.config.get('AWS', 'aws_secret_access_key')
        
        # Obtener configuración de email
        self.sender_email = self.config.get('EMAIL', 'sender_email')
        self.recipients = [email.strip() for email in self.config.get('EMAIL', 'recipients').split(',')]
        
        # Inicializar cliente SES
        self.ses_client = boto3.client(
            'ses',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
    
    def send_sync_progress_email(self, progress_percent: int, processed: int, total: int, errors: int = 0, milestone_message: str = None):
        """Envía email de progreso de sincronización Odoo → S3"""
        
        # Determinar el tipo de mensaje según el progreso
        if milestone_message:
            subject = f"🎯 Hito Alcanzado - {processed} archivos procesados"
            status_emoji = "🎯"
            status_text = "Hito Alcanzado"
        elif progress_percent == 10:
            subject = "🚀 Sincronización Odoo → S3 Iniciada (10% completado)"
            status_emoji = "🟡"
            status_text = "En Progreso"
        elif progress_percent == 50:
            subject = "⚡ Sincronización Odoo → S3 Avanzando (50% completado)"
            status_emoji = "🟠"
            status_text = "Medio Camino"
        elif progress_percent == 100:
            subject = "✅ Sincronización Odoo → S3 Completada (100%)"
            status_emoji = "🟢"
            status_text = "Completado"
        else:
            subject = f"📊 Sincronización Odoo → S3 ({progress_percent}% completado)"
            status_emoji = "🔵"
            status_text = "En Progreso"
        
        # Calcular estadísticas
        success_rate = ((processed - errors) / processed * 100) if processed > 0 else 0
        
        # Mensaje especial para hitos
        milestone_html = ""
        if milestone_message:
            milestone_html = f"""
            <div style="background: linear-gradient(135deg, #28a745, #20c997); color: white; padding: 20px; border-radius: 10px; margin: 20px 0; text-align: center;">
                <h3 style="margin: 0; font-size: 18px;">🎯 {milestone_message}</h3>
            </div>
            """
        
        # HTML del email con diseño UX
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
                .header h1 {{ margin: 0; font-size: 24px; font-weight: 300; }}
                .status-badge {{ display: inline-block; background: rgba(255,255,255,0.2); padding: 8px 16px; border-radius: 20px; margin-top: 10px; }}
                .content {{ padding: 30px; }}
                .progress-container {{ background: #f8f9fa; border-radius: 10px; padding: 20px; margin: 20px 0; }}
                .progress-bar {{ background: #e9ecef; height: 20px; border-radius: 10px; overflow: hidden; }}
                .progress-fill {{ background: linear-gradient(90deg, #28a745, #20c997); height: 100%; transition: width 0.3s ease; }}
                .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin: 20px 0; }}
                .stat-card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; border-left: 4px solid #667eea; }}
                .stat-number {{ font-size: 24px; font-weight: bold; color: #495057; }}
                .stat-label {{ font-size: 12px; color: #6c757d; text-transform: uppercase; }}
                .footer {{ background: #343a40; color: white; padding: 20px; text-align: center; font-size: 12px; }}
                .timestamp {{ color: #6c757d; font-size: 12px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{status_emoji} Sincronización OCR Spot</h1>
                    <div class="status-badge">{status_text}</div>
                </div>
                
                <div class="content">
                    <h2>Progreso de Sincronización Odoo → AWS S3</h2>
                    
                    {milestone_html}
                    
                    <div class="progress-container">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                            <span><strong>Progreso General</strong></span>
                            <span><strong>{progress_percent}%</strong></span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {progress_percent}%"></div>
                        </div>
                    </div>
                    
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-number">{processed:,}</div>
                            <div class="stat-label">Procesados</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{total:,}</div>
                            <div class="stat-label">Total</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{errors:,}</div>
                            <div class="stat-label">Errores</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{success_rate:.1f}%</div>
                            <div class="stat-label">Éxito</div>
                        </div>
                    </div>
                    
                    {"<p><strong>🎉 ¡Sincronización completada exitosamente!</strong></p><p>Todos los documentos han sido procesados y están listos para el procesamiento OCR.</p>" if progress_percent == 100 else "<p>La sincronización continúa ejecutándose. Recibirás actualizaciones adicionales del progreso.</p>"}
                    
                    <div class="timestamp">
                        📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} - Sistema OCR Spot
                    </div>
                </div>
                
                <div class="footer">
                    <p>Sistema Automatizado de Procesamiento OCR</p>
                    <p>SLEP Los Parques - ChePSS</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Texto plano como fallback
        milestone_text = f"\n🎯 {milestone_message}\n" if milestone_message else ""
        
        text_body = f"""
        {status_emoji} SINCRONIZACIÓN OCR SPOT - {status_text}
        {milestone_text}
        Progreso: {progress_percent}%
        
        Estadísticas:
        - Procesados: {processed:,} de {total:,}
        - Errores: {errors:,}
        - Tasa de éxito: {success_rate:.1f}%
        
        Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
        
        Sistema OCR Spot - SLEP Los Parques
        """
        
        return self.send_email(subject, html_body, text_body)
    
    def send_email(self, subject: str, html_body: str, text_body: str = None):
        """Envía un email usando SES"""
        try:
            # Preparar el mensaje
            message = {
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {}
            }
            
            if html_body:
                message['Body']['Html'] = {'Data': html_body, 'Charset': 'UTF-8'}
            
            if text_body:
                message['Body']['Text'] = {'Data': text_body, 'Charset': 'UTF-8'}
            
            # Enviar email
            response = self.ses_client.send_email(
                Source=self.sender_email,
                Destination={'ToAddresses': self.recipients},
                Message=message
            )
            
            print(f"✅ Email enviado exitosamente. MessageId: {response['MessageId']}")
            return True
            
        except ClientError as e:
            print(f"❌ Error enviando email: {e}")
            return False
    
    def test_email_configuration(self):
        """Prueba la configuración de email enviando un mensaje de prueba"""
        subject = "🧪 Prueba de Configuración - Sistema OCR Spot"
        
        html_body = """
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; text-align: center;">
                <h2 style="color: #28a745;">✅ Configuración de Email Exitosa</h2>
                <p>Este es un mensaje de prueba para verificar que el sistema de notificaciones por email está funcionando correctamente.</p>
                <p style="color: #6c757d; font-size: 12px;">Sistema OCR Spot - """ + datetime.now().strftime('%d/%m/%Y %H:%M:%S') + """</p>
            </div>
        </body>
        </html>
        """
        
        text_body = "✅ Configuración de email exitosa - Sistema OCR Spot funcionando correctamente."
        
        return self.send_email(subject, html_body, text_body)


def main():
    """Función de prueba"""
    mailer = SESMailer()
    
    print("Enviando email de prueba...")
    success = mailer.test_email_configuration()
    
    if success:
        print("✅ Email de prueba enviado exitosamente")
    else:
        print("❌ Error enviando email de prueba")


if __name__ == "__main__":
    main()
