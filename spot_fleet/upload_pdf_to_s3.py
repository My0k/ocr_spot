import boto3
import os
import configparser
from botocore.exceptions import ClientError
from pathlib import Path

class PDFUploader:
    def __init__(self, config_file: str = 'config.conf'):
        """Inicializa el uploader con configuración"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        # Obtener configuración
        self.region = self.config.get('AWS', 'region')
        self.table_name = self.config.get('AWS', 'dynamo_table')
        
        # Bucket de salida separado (o mismo si no se especifica)
        self.input_bucket = self.config.get('AWS', 's3_bucket')
        self.output_bucket = self.config.get('AWS', 's3_output_bucket', fallback=self.input_bucket)
        
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

    def generate_output_path(self, input_path: str, local_ocr_file: str):
        """Genera la ruta de salida en S3 basada en la ruta de entrada"""
        # Parsear input_path (s3://bucket/path/file.pdf)
        if not input_path.startswith('s3://'):
            raise ValueError("La ruta de entrada debe empezar con s3://")
        
        path_parts = input_path[5:].split('/', 1)  # Remover s3://
        bucket_name = path_parts[0]
        original_key = path_parts[1]
        
        # Generar nueva clave para el archivo con OCR
        path_obj = Path(original_key)
        name_without_ext = path_obj.stem
        extension = path_obj.suffix
        
        # Agregar sufijo _ocr al nombre
        new_filename = f"{name_without_ext}_ocr{extension}"
        new_key = str(path_obj.parent / new_filename)
        
        # Generar ruta de salida (puede ser diferente bucket)
        output_path = f"s3://{self.output_bucket}/{new_key}"
        
        print(f"📦 Bucket de salida: {self.output_bucket}")
        print(f"📄 Ruta de salida: {output_path}")
        
        return output_path, new_key

    def upload_pdf(self, local_file_path: str, input_path: str):
        """Sube el PDF con OCR a S3 y actualiza DynamoDB"""
        try:
            # Validar que el archivo local existe
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"No se encontró el archivo local: {local_file_path}")
            
            # Generar ruta de salida
            output_path, s3_key = self.generate_output_path(input_path, local_file_path)
            
            print(f"Subiendo {local_file_path} a {output_path}")
            
            # Subir archivo a S3
            self.s3_client.upload_file(
                local_file_path,
                self.output_bucket,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/pdf',
                    'Metadata': {
                        'original_file': input_path,
                        'processed_with': 'ocrmypdf'
                    }
                }
            )
            
            print(f"Archivo subido exitosamente a: {output_path}")
            
            # Actualizar DynamoDB: cambiar ocr_done de in_process a true y agregar output_path
            self.update_dynamodb_success(input_path, output_path)
            
            return output_path
            
        except ClientError as e:
            print(f"Error subiendo archivo a S3: {e}")
            self.update_dynamodb_failure(input_path)
            return None
        except Exception as e:
            print(f"Error: {e}")
            self.update_dynamodb_failure(input_path)
            return None

    def update_dynamodb_success(self, input_path: str, output_path: str):
        """Actualiza DynamoDB cuando la subida es exitosa"""
        try:
            # Eliminar registro con ocr_done = in_process
            self.table.delete_item(
                Key={
                    'input_path': input_path,
                    'ocr_done': 'in_process'
                }
            )
            
            # Crear nuevo registro con ocr_done = true y output_path
            self.table.put_item(
                Item={
                    'input_path': input_path,
                    'output_path': output_path,
                    'ocr_done': 'true',
                    'odoo_loaded': 'false'
                }
            )
            
            print(f"DynamoDB actualizado: OCR completado para {input_path}")
            
        except ClientError as e:
            print(f"Error actualizando DynamoDB: {e}")

    def update_dynamodb_failure(self, input_path: str):
        """Actualiza DynamoDB cuando hay un error en la subida"""
        try:
            # Eliminar registro con ocr_done = in_process
            self.table.delete_item(
                Key={
                    'input_path': input_path,
                    'ocr_done': 'in_process'
                }
            )
            
            # Crear nuevo registro con ocr_done = false para reintento
            self.table.put_item(
                Item={
                    'input_path': input_path,
                    'ocr_done': 'false',
                    'odoo_loaded': 'false'
                }
            )
            
            print(f"DynamoDB actualizado: Error en procesamiento, marcado para reintento")
            
        except ClientError as e:
            print(f"Error actualizando DynamoDB tras fallo: {e}")

    def cleanup_local_file(self, local_path: str):
        """Elimina el archivo local después de subirlo"""
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"Archivo local eliminado: {local_path}")
        except Exception as e:
            print(f"Error eliminando archivo local: {e}")


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
    
    if len(sys.argv) < 3:
        print("Uso: python upload_pdf_to_s3.py <archivo_local_ocr> <input_path_original>")
        sys.exit(1)
    
    local_file = sys.argv[1]
    input_path = sys.argv[2]
    
    uploader = PDFUploader(args.config)
    
    # Subir PDF con OCR
    output_path = uploader.upload_pdf(local_file, input_path)
    
    if output_path:
        print(f"Proceso completado exitosamente")
        print(f"Archivo disponible en: {output_path}")
        
        # Limpiar archivo local
        cleanup = input("¿Eliminar archivo local? (y/N): ").strip().lower()
        if cleanup == 'y':
            uploader.cleanup_local_file(local_file)
        
        return output_path
    else:
        print("Error: No se pudo completar la subida")
        return None


if __name__ == "__main__":
    import sys
    main()