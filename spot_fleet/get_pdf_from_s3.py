import boto3
import os
import configparser
from botocore.exceptions import ClientError
import random

class PDFDownloader:
    def __init__(self, config_file: str = 'config.conf'):
        """Inicializa el descargador con configuración"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        # Obtener configuración
        self.region = self.config.get('AWS', 'region')
        self.table_name = self.config.get('AWS', 'dynamo_table')
        self.aws_access_key_id = self.config.get('AWS', 'aws_access_key_id')
        self.aws_secret_access_key = self.config.get('AWS', 'aws_secret_access_key')
        
        # Inicializar clientes AWS
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
        
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
        
        self.table = self.dynamodb.Table(self.table_name)
        
        # Crear directorio local para PDFs
        self.local_dir = 'pdfs_to_process'
        os.makedirs(self.local_dir, exist_ok=True)

    def get_available_pdf(self):
        """Busca un PDF disponible para procesar (ocr_done = false) y lo marca como in_process"""
        try:
            # Buscar PDFs con ocr_done = false
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('false')
            )
            
            if not response['Items']:
                print("No hay PDFs disponibles para procesar")
                return None
            
            # Seleccionar un PDF al azar para evitar conflictos entre instancias
            pdf_item = random.choice(response['Items'])
            input_path = pdf_item['input_path']
            
            print(f"Seleccionado para procesar: {input_path}")
            
            # Intentar cambiar el estado a in_process
            try:
                # Eliminar registro con ocr_done = false
                self.table.delete_item(
                    Key={
                        'input_path': input_path,
                        'ocr_done': 'false'
                    }
                )
                
                # Crear nuevo registro con ocr_done = in_process
                self.table.put_item(
                    Item={
                        'input_path': input_path,
                        'ocr_done': 'in_process',
                        'odoo_loaded': pdf_item.get('odoo_loaded', 'false')
                    }
                )
                
                print(f"Estado cambiado a 'in_process' para: {input_path}")
                return input_path
                
            except ClientError as e:
                print(f"Error cambiando estado: {e}")
                return None
                
        except ClientError as e:
            print(f"Error buscando PDFs disponibles: {e}")
            return None

    def download_pdf(self, s3_path: str):
        """Descarga un PDF desde S3 al directorio local"""
        try:
            # Parsear la ruta S3
            if not s3_path.startswith('s3://'):
                raise ValueError("La ruta debe empezar con s3://")
            
            path_parts = s3_path[5:].split('/', 1)  # Remover s3:// y dividir
            bucket_name = path_parts[0]
            key = path_parts[1]
            
            # Generar nombre de archivo local
            filename = os.path.basename(key)
            local_path = os.path.join(self.local_dir, filename)
            
            # Descargar archivo
            print(f"Descargando {s3_path} a {local_path}")
            self.s3_client.download_file(bucket_name, key, local_path)
            
            print(f"PDF descargado exitosamente: {local_path}")
            return local_path
            
        except ClientError as e:
            print(f"Error descargando PDF: {e}")
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    def process_next_pdf(self):
        """Busca y descarga el siguiente PDF disponible para procesar"""
        # Obtener PDF disponible
        s3_path = self.get_available_pdf()
        if not s3_path:
            return None
        
        # Descargar PDF
        local_path = self.download_pdf(s3_path)
        if not local_path:
            # Si falla la descarga, revertir el estado en DynamoDB
            self.revert_status(s3_path)
            return None
        
        return {
            'input_path': s3_path,
            'local_path': local_path
        }

    def revert_status(self, input_path: str):
        """Revierte el estado de in_process a false si hay un error"""
        try:
            # Eliminar registro con ocr_done = in_process
            self.table.delete_item(
                Key={
                    'input_path': input_path,
                    'ocr_done': 'in_process'
                }
            )
            
            # Crear nuevo registro con ocr_done = false
            self.table.put_item(
                Item={
                    'input_path': input_path,
                    'ocr_done': 'false',
                    'odoo_loaded': 'false'
                }
            )
            
            print(f"Estado revertido a 'false' para: {input_path}")
            
        except ClientError as e:
            print(f"Error revirtiendo estado: {e}")


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Procesador completo OCR para PDFs')
    parser.add_argument('--language', '-l', default='spa', 
                       help='Idioma para OCR (default: spa)')
    parser.add_argument('--continuous', '-c', action='store_true',
                       help='Procesar PDFs continuamente')
    parser.add_argument('--max-iterations', '-m', type=int,
                       help='Máximo número de iteraciones en modo continuo')
    parser.add_argument('--config', default='config.conf',
                       help='Archivo de configuración (default: config.conf)')
    
    args = parser.parse_args()
    
    downloader = PDFDownloader(args.config)
    
    # Procesar siguiente PDF disponible
    result = downloader.process_next_pdf()
    
    if result:
        print(f"PDF listo para procesar:")
        print(f"- Ruta S3: {result['input_path']}")
        print(f"- Ruta local: {result['local_path']}")
        return result
    else:
        print("No se pudo obtener ningún PDF para procesar")
        return None


if __name__ == "__main__":
    main()
