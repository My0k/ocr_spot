import boto3
import json
import os
from botocore.exceptions import ClientError
import configparser
from typing import List, Dict, Any
from datetime import datetime
import unicodedata
import re

class OCRSpotManager:
    def __init__(self, config_file: str = 'config.conf'):
        """Inicializa el manager con configuración desde archivo"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        # Obtener configuración
        self.region = self.config.get('AWS', 'region')
        self.bucket_name = self.config.get('AWS', 's3_bucket')
        self.output_bucket = self.config.get('AWS', 'output_bucket')  # Agregar bucket de salida
        self.table_name = self.config.get('AWS', 'dynamo_table')
        self.sns_topic_arn = self.config.get('AWS', 'sns_topic_arn')
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
        
        self.sns_client = boto3.client(
            'sns',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
        
        # Configuración de tabla DynamoDB
        self.table = self.dynamodb.Table(self.table_name)

    def listen_sns_messages(self, queue_name: str = None):
        """Escucha mensajes de SNS usando una cola SQS existente o creando una nueva si es posible"""
        print(f"Configurando escucha de mensajes SNS en: {self.sns_topic_arn}")
        
        sqs = boto3.client(
            'sqs',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
        
        try:
            if queue_name:
                # Usar cola existente
                try:
                    response = sqs.get_queue_url(QueueName=queue_name)
                    queue_url = response['QueueUrl']
                    print(f"Usando cola existente: {queue_name}")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                        print(f"La cola {queue_name} no existe. Intentando crear una nueva...")
                        response = sqs.create_queue(QueueName=queue_name)
                        queue_url = response['QueueUrl']
                    else:
                        raise e
            else:
                # Intentar crear cola temporal
                queue_name = f"ocr-spot-temp-{os.getpid()}"
                response = sqs.create_queue(QueueName=queue_name)
                queue_url = response['QueueUrl']
            
            # Obtener ARN de la cola
            queue_attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['QueueArn', 'ApproximateNumberOfMessages']
            )
            queue_arn = queue_attrs['Attributes']['QueueArn']
            message_count = queue_attrs['Attributes']['ApproximateNumberOfMessages']
            
            print(f"ARN de la cola: {queue_arn}")
            print(f"Mensajes aproximados en cola: {message_count}")
            
            # Configurar política de acceso para la cola SQS
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "sqs:SendMessage",
                        "Resource": queue_arn,
                        "Condition": {
                            "ArnEquals": {
                                "aws:SourceArn": self.sns_topic_arn
                            }
                        }
                    }
                ]
            }
            
            try:
                sqs.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes={
                        'Policy': json.dumps(policy)
                    }
                )
                print("Política de acceso configurada para la cola")
            except ClientError as e:
                print(f"Advertencia: No se pudo configurar la política de la cola: {e}")
            
            # Verificar si ya está suscrita al topic
            subscriptions = self.sns_client.list_subscriptions_by_topic(
                TopicArn=self.sns_topic_arn
            )
            
            already_subscribed = False
            subscription_arn = None
            for sub in subscriptions['Subscriptions']:
                if sub['Endpoint'] == queue_arn:
                    already_subscribed = True
                    subscription_arn = sub['SubscriptionArn']
                    break
            
            if not already_subscribed:
                # Suscribir cola al topic SNS
                sub_response = self.sns_client.subscribe(
                    TopicArn=self.sns_topic_arn,
                    Protocol='sqs',
                    Endpoint=queue_arn
                )
                subscription_arn = sub_response['SubscriptionArn']
                print(f"Cola suscrita al topic SNS: {subscription_arn}")
            else:
                print(f"Cola ya estaba suscrita al topic: {subscription_arn}")
            
            # Confirmar suscripción si es necesario
            if subscription_arn and subscription_arn != 'PendingConfirmation':
                print("Suscripción confirmada")
            elif subscription_arn == 'PendingConfirmation':
                print("Suscripción pendiente de confirmación")
            
            print(f"Escuchando mensajes en cola: {queue_url}")
            print("Presiona Ctrl+C para detener...")
            
            message_received_count = 0
            poll_count = 0
            
            while True:
                poll_count += 1
                print(f"Poll #{poll_count} - Esperando mensajes...")
                
                messages = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    AttributeNames=['All'],
                    MessageAttributeNames=['All']
                )
                
                if 'Messages' in messages:
                    for message in messages['Messages']:
                        message_received_count += 1
                        print(f"\n--- MENSAJE #{message_received_count} ---")
                        print(f"MessageId: {message.get('MessageId', 'N/A')}")
                        print(f"ReceiptHandle: {message.get('ReceiptHandle', 'N/A')[:50]}...")
                        
                        try:
                            # Parsear el mensaje SNS
                            message_body = message['Body']
                            print(f"Raw Body: {message_body}")
                            
                            sns_message = json.loads(message_body)
                            print(f"SNS Message Type: {sns_message.get('Type', 'Unknown')}")
                            
                            if sns_message.get('Type') == 'Notification' and 'Message' in sns_message:
                                try:
                                    actual_message = json.loads(sns_message['Message'])
                                    print(f"Parsed Message: {json.dumps(actual_message, indent=2)}")
                                except json.JSONDecodeError:
                                    print(f"Message (not JSON): {sns_message['Message']}")
                            elif sns_message.get('Type') == 'SubscriptionConfirmation':
                                print("Mensaje de confirmación de suscripción recibido")
                                if 'SubscribeURL' in sns_message:
                                    print(f"URL de confirmación: {sns_message['SubscribeURL']}")
                            else:
                                print(f"Mensaje SNS completo: {json.dumps(sns_message, indent=2)}")
                                
                        except json.JSONDecodeError:
                            print(f"Mensaje no es JSON válido: {message['Body']}")
                        
                        # Eliminar mensaje procesado
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print("Mensaje eliminado de la cola")
                else:
                    print("No hay mensajes nuevos")
                    
        except KeyboardInterrupt:
            print(f"\nDeteniendo escucha de mensajes...")
            print(f"Total de mensajes procesados: {message_received_count}")
        except ClientError as e:
            if 'AccessDenied' in str(e) and 'createqueue' in str(e).lower():
                print(f"\nError de permisos: No se puede crear cola SQS.")
                print("Opciones:")
                print("1. Pide al administrador AWS que agregue permisos sqs:CreateQueue")
                print("2. Usa una cola SQS existente especificando su nombre")
                print("3. Crea la cola manualmente en la consola AWS")
                
                existing_queue = input("\n¿Tienes una cola SQS existente? Ingresa el nombre (o Enter para salir): ").strip()
                if existing_queue:
                    return self.listen_sns_messages(existing_queue)
            else:
                print(f"Error: {e}")
        finally:
            # Solo intentar eliminar si creamos una cola temporal
            if not queue_name or queue_name.startswith('ocr-spot-temp-'):
                try:
                    sqs.delete_queue(QueueUrl=queue_url)
                    print("Cola temporal eliminada")
                except:
                    pass

    def find_all_pdfs_in_bucket(self, prefix: str = '') -> List[str]:
        """Encuentra recursivamente todos los PDFs en el bucket configurado"""
        pdf_paths = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.lower().endswith('.pdf'):
                            pdf_paths.append(key)
            
            print(f"Encontrados {len(pdf_paths)} archivos PDF en s3://{self.bucket_name}/{prefix}")
            return pdf_paths
            
        except ClientError as e:
            print(f"Error accediendo al bucket {self.bucket_name}: {e}")
            return []

    def generate_dynamodb_entries(self, prefix: str = ''):
        """Genera entradas en DynamoDB para todos los PDFs encontrados en el bucket configurado"""
        pdf_paths = self.find_all_pdfs_in_bucket(prefix)
        
        if not pdf_paths:
            print("No se encontraron archivos PDF")
            return
        
        new_entries = 0
        existing_entries = 0
        
        for pdf_path in pdf_paths:
            input_path = f"s3://{self.bucket_name}/{pdf_path}"
            
            # Generar output_path automáticamente con sufijo _ocr
            from pathlib import Path
            path_obj = Path(pdf_path)
            name_without_ext = path_obj.stem
            extension = path_obj.suffix
            output_filename = f"{name_without_ext}_ocr{extension}"
            output_key = str(path_obj.parent / output_filename)
            output_path = f"s3://{self.output_bucket}/{output_key}"
            
            try:
                # Verificar si ya existe en DynamoDB usando ambas claves
                response = self.table.get_item(
                    Key={
                        'input_path': input_path,
                        'ocr_done': 'false'  # Intentar con estado false primero
                    }
                )
                
                # Si no existe con false, verificar con in_process
                if 'Item' not in response:
                    response = self.table.get_item(
                        Key={
                            'input_path': input_path,
                            'ocr_done': 'in_process'
                        }
                    )
                
                # Si no existe con in_process, verificar con true
                if 'Item' not in response:
                    response = self.table.get_item(
                        Key={
                            'input_path': input_path,
                            'ocr_done': 'true'
                        }
                    )
                
                if 'Item' not in response:
                    # No existe, crear nueva entrada con output_path
                    self.table.put_item(
                        Item={
                            'input_path': input_path,
                            'output_path': output_path,
                            'ocr_done': 'false',
                            'odoo_loaded': 'false'
                        }
                    )
                    new_entries += 1
                    print(f"✅ Nueva entrada: {input_path}")
                    print(f"   → Output: {output_path}")
                else:
                    existing_entries += 1
                    # Si existe pero no tiene output_path, agregarlo
                    if 'output_path' not in response['Item']:
                        try:
                            self.table.update_item(
                                Key={
                                    'input_path': input_path,
                                    'ocr_done': response['Item']['ocr_done']
                                },
                                UpdateExpression='SET output_path = :output_path',
                                ExpressionAttributeValues={
                                    ':output_path': output_path
                                }
                            )
                            print(f"🔄 Actualizado con output_path: {input_path}")
                            print(f"   → Output: {output_path}")
                        except ClientError as e:
                            print(f"❌ Error actualizando output_path: {e}")
                    else:
                        print(f"⏭️  Ya existe: {input_path} (ocr_done: {response['Item']['ocr_done']})")
                    
            except ClientError as e:
                print(f"❌ Error procesando {input_path}: {e}")
        
        print(f"\n{'='*60}")
        print(f"RESUMEN:")
        print(f"{'='*60}")
        print(f"✅ Nuevas entradas creadas: {new_entries}")
        print(f"⏭️  Entradas existentes: {existing_entries}")
        print(f"📊 Total PDFs procesados: {len(pdf_paths)}")
        print(f"📁 Bucket input: {self.bucket_name}")
        print(f"📁 Bucket output: {self.output_bucket}")
        print(f"{'='*60}")

    def reset_ocr_done_status(self):
        """Cambia todos los 'in_process' de ocr_done a 'false'"""
        print("Cambiando estados 'in_process' de ocr_done a 'false'...")
        
        try:
            # Escanear toda la tabla buscando registros con ocr_done = 'in_process'
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('in_process')
            )
            
            updated_count = 0
            
            for item in response['Items']:
                # Eliminar el registro actual
                self.table.delete_item(
                    Key={
                        'input_path': item['input_path'],
                        'ocr_done': 'in_process'
                    }
                )
                
                # Crear nuevo registro con ocr_done = 'false'
                self.table.put_item(
                    Item={
                        'input_path': item['input_path'],
                        'ocr_done': 'false',
                        'odoo_loaded': item.get('odoo_loaded', 'false')
                    }
                )
                updated_count += 1
                print(f"Actualizado: {item['input_path']}")
            
            # Manejar paginación si hay más elementos
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression=boto3.dynamodb.conditions.Attr('ocr_done').eq('in_process'),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                for item in response['Items']:
                    # Eliminar el registro actual
                    self.table.delete_item(
                        Key={
                            'input_path': item['input_path'],
                            'ocr_done': 'in_process'
                        }
                    )
                    
                    # Crear nuevo registro con ocr_done = 'false'
                    self.table.put_item(
                        Item={
                            'input_path': item['input_path'],
                            'ocr_done': 'false',
                            'odoo_loaded': item.get('odoo_loaded', 'false')
                        }
                    )
                    updated_count += 1
                    print(f"Actualizado: {item['input_path']}")
            
            print(f"\nTotal de registros actualizados (ocr_done): {updated_count}")
            
        except ClientError as e:
            print(f"Error actualizando ocr_done: {e}")

    def reset_odoo_loaded_status(self):
        """Cambia todos los 'in_process' de odoo_loaded a 'false'"""
        print("Cambiando estados 'in_process' de odoo_loaded a 'false'...")
        
        try:
            # Escanear toda la tabla buscando registros con odoo_loaded = 'in_process'
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('odoo_loaded').eq('in_process')
            )
            
            updated_count = 0
            
            for item in response['Items']:
                # Actualizar el registro manteniendo las claves
                self.table.update_item(
                    Key={
                        'input_path': item['input_path'],
                        'ocr_done': item['ocr_done']
                    },
                    UpdateExpression='SET odoo_loaded = :val',
                    ExpressionAttributeValues={':val': 'false'}
                )
                updated_count += 1
                print(f"Actualizado: {item['input_path']}")
            
            # Manejar paginación si hay más elementos
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression=boto3.dynamodb.conditions.Attr('odoo_loaded').eq('in_process'),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                for item in response['Items']:
                    self.table.update_item(
                        Key={
                            'input_path': item['input_path'],
                            'ocr_done': item['ocr_done']
                        },
                        UpdateExpression='SET odoo_loaded = :val',
                        ExpressionAttributeValues={':val': 'false'}
                    )
                    updated_count += 1
                    print(f"Actualizado: {item['input_path']}")
            
            print(f"\nTotal de registros actualizados (odoo_loaded): {updated_count}")
            
        except ClientError as e:
            print(f"Error actualizando odoo_loaded: {e}")

    def list_sqs_queues(self):
        """Lista todas las colas SQS disponibles"""
        sqs = boto3.client(
            'sqs',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )
        
        try:
            response = sqs.list_queues()
            if 'QueueUrls' in response:
                print(f"\nColas SQS disponibles en {self.region}:")
                for i, queue_url in enumerate(response['QueueUrls'], 1):
                    queue_name = queue_url.split('/')[-1]
                    print(f"{i}. {queue_name}")
                    print(f"   URL: {queue_url}")
            else:
                print("No se encontraron colas SQS")
        except ClientError as e:
            print(f"Error listando colas SQS: {e}")

    def send_test_message(self):
        """Envía un mensaje de prueba al topic SNS"""
        test_message = {
            "test": True,
            "message": "Mensaje de prueba desde main_tools.py",
            "timestamp": json.dumps({"default": str(os.time())})
        }
        
        try:
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Message=json.dumps(test_message),
                MessageGroupId="test-group",
                MessageDeduplicationId=f"test-{os.getpid()}-{int(os.time())}"
            )
            
            print(f"Mensaje de prueba enviado exitosamente:")
            print(f"MessageId: {response['MessageId']}")
            print(f"Contenido: {json.dumps(test_message, indent=2)}")
            
        except ClientError as e:
            print(f"Error enviando mensaje de prueba: {e}")

    def delete_bucket_contents(self):
        """Elimina todo el contenido del bucket S3 configurado"""
        print(f"\n⚠️  ADVERTENCIA: Esta operación eliminará TODOS los archivos del bucket:")
        print(f"   Bucket: s3://{self.bucket_name}")
        print(f"   Esta acción NO se puede deshacer")
        
        confirmation = input("\nPara continuar, escribe exactamente 'ELIMINAR_BUCKET': ").strip()
        
        if confirmation != 'ELIMINAR_BUCKET':
            print("❌ Operación cancelada - confirmación incorrecta")
            return
        
        try:
            # Obtener lista de todos los objetos
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name)
            
            deleted_count = 0
            
            for page in pages:
                if 'Contents' in page:
                    # Preparar lista de objetos para eliminar
                    objects_to_delete = []
                    for obj in page['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})
                    
                    if objects_to_delete:
                        # Eliminar objetos en lotes
                        response = self.s3_client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={
                                'Objects': objects_to_delete,
                                'Quiet': False
                            }
                        )
                        
                        # Contar eliminados exitosamente
                        if 'Deleted' in response:
                            deleted_count += len(response['Deleted'])
                            for deleted in response['Deleted']:
                                print(f"Eliminado: {deleted['Key']}")
                        
                        # Mostrar errores si los hay
                        if 'Errors' in response:
                            for error in response['Errors']:
                                print(f"Error eliminando {error['Key']}: {error['Message']}")
            
            print(f"\n✅ Operación completada")
            print(f"   Total de archivos eliminados: {deleted_count}")
            
        except ClientError as e:
            print(f"❌ Error eliminando contenido del bucket: {e}")

    def delete_dynamodb_table_contents(self):
        """Elimina todo el contenido de la tabla DynamoDB configurada"""
        print(f"\n⚠️  ADVERTENCIA: Esta operación eliminará TODOS los registros de la tabla:")
        print(f"   Tabla: {self.table_name}")
        print(f"   Esta acción NO se puede deshacer")
        
        confirmation = input("\nPara continuar, escribe exactamente 'ELIMINAR_TABLA': ").strip()
        
        if confirmation != 'ELIMINAR_TABLA':
            print("❌ Operación cancelada - confirmación incorrecta")
            return
        
        try:
            # Escanear toda la tabla para obtener todas las claves
            response = self.table.scan()
            deleted_count = 0
            
            while True:
                items = response.get('Items', [])
                
                if not items:
                    break
                
                # Eliminar items en lotes usando batch_writer
                with self.table.batch_writer() as batch:
                    for item in items:
                        # Usar las claves primarias de la tabla (input_path y ocr_done)
                        batch.delete_item(
                            Key={
                                'input_path': item['input_path'],
                                'ocr_done': item['ocr_done']
                            }
                        )
                        deleted_count += 1
                        print(f"Eliminado: {item['input_path']} (ocr_done: {item['ocr_done']})")
                
                # Continuar con la siguiente página si existe
                if 'LastEvaluatedKey' in response:
                    response = self.table.scan(
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                else:
                    break
            
            print(f"\n✅ Operación completada")
            print(f"   Total de registros eliminados: {deleted_count}")
            
        except ClientError as e:
            print(f"❌ Error eliminando contenido de la tabla: {e}")

    def normalize_s3_path(self, text: str) -> str:
        """Normaliza texto para uso en S3, eliminando tildes y caracteres especiales"""
        if not text:
            return text
        
        # Diccionario de reemplazos específicos para caracteres comunes
        replacements = {
            # Vocales con tildes
            'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ā': 'a', 'ă': 'a', 'ą': 'a',
            'Á': 'A', 'À': 'A', 'Ä': 'A', 'Â': 'A', 'Ā': 'A', 'Ă': 'A', 'Ą': 'A',
            
            'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e', 'ē': 'e', 'ĕ': 'e', 'ė': 'e', 'ę': 'e', 'ě': 'e',
            'É': 'E', 'È': 'E', 'Ë': 'E', 'Ê': 'E', 'Ē': 'E', 'Ĕ': 'E', 'Ė': 'E', 'Ę': 'E', 'Ě': 'E',
            
            'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i', 'ī': 'i', 'ĭ': 'i', 'į': 'i',
            'Í': 'I', 'Ì': 'I', 'Ï': 'I', 'Î': 'I', 'Ī': 'I', 'Ĭ': 'I', 'Į': 'I',
            
            'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'ō': 'o', 'ŏ': 'o', 'ő': 'o', 'ø': 'o',
            'Ó': 'O', 'Ò': 'O', 'Ö': 'O', 'Ô': 'O', 'Ō': 'O', 'Ŏ': 'O', 'Ő': 'O', 'Ø': 'O',
            
            'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u', 'ū': 'u', 'ŭ': 'u', 'ů': 'u', 'ű': 'u', 'ų': 'u',
            'Ú': 'U', 'Ù': 'U', 'Ü': 'U', 'Û': 'U', 'Ū': 'U', 'Ŭ': 'U', 'Ů': 'U', 'Ű': 'U', 'Ų': 'U',
            
            # Consonantes especiales
            'ñ': 'n', 'Ñ': 'N',
            'ç': 'c', 'Ç': 'C', 'ć': 'c', 'Ć': 'C', 'č': 'c', 'Č': 'C',
            'ş': 's', 'Ş': 'S', 'š': 's', 'Š': 'S', 'ś': 's', 'Ś': 'S',
            'ž': 'z', 'Ž': 'Z', 'ź': 'z', 'Ź': 'Z', 'ż': 'z', 'Ż': 'Z',
            'ř': 'r', 'Ř': 'R',
            'ł': 'l', 'Ł': 'L', 'ľ': 'l', 'Ľ': 'L',
            'ď': 'd', 'Ď': 'D',
            'ť': 't', 'Ť': 'T',
            'ň': 'n', 'Ň': 'N', 'ń': 'n', 'Ń': 'N',
            'ğ': 'g', 'Ğ': 'G',
            'ı': 'i', 'İ': 'I',
            
            # Otros caracteres comunes
            'æ': 'ae', 'Æ': 'AE',
            'œ': 'oe', 'Œ': 'OE',
            'ß': 'ss',
            'ð': 'd', 'Ð': 'D',
            'þ': 'th', 'Þ': 'TH',
            
            # Símbolos y puntuación que pueden causar problemas
            '–': '-', '—': '-', '―': '-',  # Diferentes tipos de guiones
            ''': "'", ''': "'", '"': '"', '"': '"',  # Comillas tipográficas
            '…': '...', '•': '-', '·': '-',
            '°': 'deg', '®': 'R', '©': 'C', '™': 'TM',
            '€': 'EUR', '$': 'USD', '£': 'GBP', '¥': 'JPY',
            '&': 'and', '@': 'at', '#': 'num', '%': 'pct',
            '+': 'plus', '=': 'eq', '<': 'lt', '>': 'gt',
            '¿': '', '¡': '', '«': '', '»': '',
            
            # Espacios especiales
            '\u00A0': ' ',  # Non-breaking space
            '\u2000': ' ', '\u2001': ' ', '\u2002': ' ', '\u2003': ' ',  # En spaces
            '\u2004': ' ', '\u2005': ' ', '\u2006': ' ', '\u2007': ' ',  # Em spaces
            '\u2008': ' ', '\u2009': ' ', '\u200A': ' ', '\u200B': '',   # Thin spaces
        }
        
        # Aplicar reemplazos específicos
        normalized_text = text
        for original, replacement in replacements.items():
            normalized_text = normalized_text.replace(original, replacement)
        
        # Reemplazar espacios múltiples con uno solo
        normalized_text = re.sub(r'\s+', ' ', normalized_text)
        
        # Reemplazar espacios con guiones bajos
        normalized_text = normalized_text.replace(' ', '_')
        
        # Eliminar caracteres especiales restantes, mantener solo alfanuméricos, puntos, guiones y guiones bajos
        normalized_text = re.sub(r'[^a-zA-Z0-9._/-]', '', normalized_text)
        
        # Limpiar guiones bajos múltiples
        normalized_text = re.sub(r'_+', '_', normalized_text)
        
        # Eliminar guiones bajos al inicio y final
        normalized_text = normalized_text.strip('_')
        
        return normalized_text

    def sync_odoo_to_s3_and_dynamodb(self, step_by_step: bool = True):
        """Sincroniza PDFs desde Odoo a S3 y genera entradas en DynamoDB"""
        import xmlrpc.client
        import base64
        import tempfile
        from pathlib import Path
        from send_mail import SESMailer
        
        # Obtener configuración Odoo desde config.conf
        ODOO_URL = self.config.get('ODOO', 'url')
        ODOO_DB = self.config.get('ODOO', 'database')
        ODOO_USER = self.config.get('ODOO', 'username')
        ODOO_PASSWORD = self.config.get('ODOO', 'password')
        
        # Inicializar mailer
        mailer = SESMailer()
        
        print("=== SINCRONIZACIÓN ODOO → S3 → DYNAMODB ===")
        print("Conectando a Odoo...")
        
        try:
            # Conexión XML-RPC
            common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
            uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
            
            if not uid:
                print("❌ Error de autenticación en Odoo")
                return
            
            models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
            
            # Detectar campo binario
            print("Detectando campo binario en gesdocs.document...")
            fields = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                'gesdocs.document', 'fields_get',
                [], {'attributes': ['string', 'type']}
            )
            
            binary_fields = [f for f, info in fields.items() if info.get('type') == 'binary']
            if not binary_fields:
                print("❌ No se encontraron campos binarios en gesdocs.document")
                return
            
            BINARY_FIELD = binary_fields[0]
            print(f"✅ Campo binario detectado: {BINARY_FIELD}")
            
            # Obtener documentos
            document_ids = models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                'gesdocs.document', 'search',
                [[]],
                {'order': 'id asc'}
            )
            
            print(f"\nSe encontraron {len(document_ids)} documentos en Odoo")
            
            processed_count = 0
            skipped_count = 0
            error_count = 0
            total_docs = len(document_ids)
            
            # Variables para control de emails
            email_10_sent = False
            email_50_sent = False
            email_100_files_sent = False  # Nuevo control para 100 archivos
            
            for index, doc_id in enumerate(document_ids, start=1):
                try:
                    # Calcular progreso
                    progress_percent = (index / total_docs) * 100
                    
                    # Enviar emails de progreso
                    if progress_percent >= 10 and not email_10_sent:
                        print("\n📧 Enviando notificación de progreso (10%)...")
                        mailer.send_sync_progress_email(10, index, total_docs, error_count)
                        email_10_sent = True
                    
                    if progress_percent >= 50 and not email_50_sent:
                        print("\n📧 Enviando notificación de progreso (50%)...")
                        mailer.send_sync_progress_email(50, index, total_docs, error_count)
                        email_50_sent = True
                    
                    # Enviar email cuando se hayan procesado 100 archivos exitosamente
                    if processed_count == 100 and not email_100_files_sent:
                        print("\n📧 Enviando notificación de 100 archivos procesados...")
                        mailer.send_sync_progress_email(
                            int((index / total_docs) * 100), 
                            index, 
                            total_docs, 
                            error_count,
                            milestone_message="¡Primer hito alcanzado! 100 archivos procesados exitosamente."
                        )
                        email_100_files_sent = True
                    
                    # Leer datos del documento
                    doc_data = models.execute_kw(
                        ODOO_DB, uid, ODOO_PASSWORD,
                        'gesdocs.document', 'read',
                        [doc_id],
                        {'fields': ['employee_id', 'tipo_documento', BINARY_FIELD, 'name']}
                    )[0]
                    
                    emp_id = doc_data.get('employee_id') and doc_data['employee_id'][0] or False
                    tipo_doc = doc_data.get('tipo_documento') or 'Desconocido'
                    
                    if not emp_id or not doc_data.get(BINARY_FIELD):
                        print(f"[{index}/{total_docs}] ⚠️ Documento {doc_id} sin empleado o archivo, saltando")
                        skipped_count += 1
                        continue
                    
                    # Leer datos del empleado
                    emp_data = models.execute_kw(
                        ODOO_DB, uid, ODOO_PASSWORD,
                        'hr.employee', 'read',
                        [emp_id],
                        {'fields': ['comuna_id', 'job_id', 'address_id', 'identification_id']}
                    )[0]
                    
                    comuna = emp_data.get('comuna_id') and emp_data['comuna_id'][1] or 'SinComuna'
                    tipologia = emp_data.get('job_id') and emp_data['job_id'][1] or 'SinCargo'
                    rbd = emp_data.get('address_id') and emp_data['address_id'][1] or 'SinRBD'
                    rut = emp_data.get('identification_id') or 'SinRUT'
                    
                    # Normalizar nombres para S3 (eliminar tildes y caracteres especiales)
                    comuna_norm = self.normalize_s3_path(comuna)
                    tipologia_norm = self.normalize_s3_path(tipologia)
                    rbd_norm = self.normalize_s3_path(rbd)
                    rut_norm = self.normalize_s3_path(rut)
                    tipo_doc_norm = self.normalize_s3_path(tipo_doc)
                    
                    # Generar rutas S3 (input y output) con nombres normalizados
                    s3_key = f"{comuna_norm}/{tipologia_norm}/{rbd_norm}/{rut_norm}/{tipo_doc_norm}.pdf"
                    input_s3_path = f"s3://{self.bucket_name}/{s3_key}"
                    
                    # Generar output path con sufijo _ocr
                    path_obj = Path(s3_key)
                    name_without_ext = path_obj.stem
                    extension = path_obj.suffix
                    output_filename = f"{name_without_ext}_ocr{extension}"
                    output_s3_key = str(path_obj.parent / output_filename)
                    output_s3_path = f"s3://{self.output_bucket}/{output_s3_key}"
                    
                    # Verificar si ya existe en S3
                    try:
                        self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                        print(f"[{index}/{total_docs}] ⏭️ Ya existe en S3: {s3_key}")
                        
                        # Verificar/crear entrada en DynamoDB con output_path
                        self._ensure_dynamodb_entry(input_s3_path, output_s3_path)
                        skipped_count += 1
                        
                        if step_by_step:
                            input("Presiona ENTER para continuar...")
                        continue
                        
                    except ClientError as e:
                        if e.response['Error']['Code'] != '404':
                            print(f"Error verificando S3: {e}")
                            continue
                    
                    # Descargar y subir PDF
                    try:
                        pdf_data = base64.b64decode(doc_data[BINARY_FIELD])
                        
                        # Crear archivo temporal
                        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                            temp_file.write(pdf_data)
                            temp_path = temp_file.name
                        
                        # Subir a S3 con metadatos normalizados (solo ASCII)
                        self.s3_client.upload_file(
                            temp_path,
                            self.bucket_name,
                            s3_key,
                            ExtraArgs={
                                'ContentType': 'application/pdf',
                                'Metadata': {
                                    'source': 'odoo',
                                    'doc_id': str(doc_id),
                                    'employee_id': str(emp_id),
                                    'tipo_documento': tipo_doc_norm,
                                    'comuna_norm': comuna_norm,
                                    'tipologia_norm': tipologia_norm,
                                    'rbd_norm': rbd_norm
                                }
                            }
                        )
                        
                        # Eliminar archivo temporal
                        os.unlink(temp_path)
                        
                        # Crear entrada en DynamoDB con output_path
                        self._ensure_dynamodb_entry(input_s3_path, output_s3_path)
                        
                        print(f"[{index}/{total_docs}] ✅ Procesado: {s3_key}")
                        processed_count += 1
                        
                    except Exception as e:
                        print(f"[{index}/{total_docs}] ❌ Error procesando: {e}")
                        error_count += 1
                    
                    if step_by_step:
                        input("Presiona ENTER para continuar...")
                        
                except Exception as e:
                    print(f"[{index}/{total_docs}] ❌ Error general: {e}")
                    error_count += 1
                    if step_by_step:
                        input("Presiona ENTER para continuar...")
            
            # Enviar email de finalización (100%)
            print("\n📧 Enviando notificación de finalización (100%)...")
            mailer.send_sync_progress_email(100, total_docs, total_docs, error_count)
            
            print(f"\n=== RESUMEN DE SINCRONIZACIÓN ===")
            print(f"✅ Procesados: {processed_count}")
            print(f"⏭️ Saltados (ya existían): {skipped_count}")
            print(f"❌ Errores: {error_count}")
            print(f"📊 Total: {total_docs}")
            print(f"📁 Bucket input: {self.bucket_name}")
            print(f"📁 Bucket output: {self.output_bucket}")
            print(f"📧 Notificaciones enviadas a: {', '.join(mailer.recipients)}")
            
        except Exception as e:
            print(f"❌ Error en sincronización: {e}")

    def _ensure_dynamodb_entry(self, input_s3_path: str, output_s3_path: str = None):
        """Asegura que existe una entrada en DynamoDB para el archivo S3"""
        try:
            # Verificar si ya existe con cualquier estado
            states_to_check = ['false', 'in_process', 'true']
            exists = False
            existing_item = None
            
            for state in states_to_check:
                try:
                    response = self.table.get_item(
                        Key={
                            'input_path': input_s3_path,
                            'ocr_done': state
                        }
                    )
                    if 'Item' in response:
                        exists = True
                        existing_item = response['Item']
                        break
                except ClientError:
                    continue
            
            if not exists:
                # Crear nueva entrada con output_path
                item_data = {
                    'input_path': input_s3_path,
                    'ocr_done': 'false',
                    'odoo_loaded': 'false'
                }
                
                if output_s3_path:
                    item_data['output_path'] = output_s3_path
                
                self.table.put_item(Item=item_data)
                
            elif existing_item and output_s3_path and 'output_path' not in existing_item:
                # Actualizar entrada existente para agregar output_path si no lo tiene
                try:
                    self.table.update_item(
                        Key={
                            'input_path': input_s3_path,
                            'ocr_done': existing_item['ocr_done']
                        },
                        UpdateExpression='SET output_path = :output_path',
                        ExpressionAttributeValues={
                            ':output_path': output_s3_path
                        }
                    )
                except ClientError as e:
                    print(f"Error actualizando output_path: {e}")
                
        except ClientError as e:
            print(f"Error creando entrada DynamoDB para {input_s3_path}: {e}")


def main():
    """Función principal con menú interactivo"""
    try:
        manager = OCRSpotManager()
        print(f"Configuración cargada:")
        print(f"- Bucket S3 Input: {manager.bucket_name}")
        print(f"- Bucket S3 Output: {manager.output_bucket}")
        print(f"- Tabla DynamoDB: {manager.table_name}")
        print(f"- Región: {manager.region}")
        print(f"- Topic SNS: {manager.sns_topic_arn}")
    except Exception as e:
        print(f"Error cargando configuración: {e}")
        return
    
    while True:
        print("\n" + "="*50)
        print("OCR SPOT MANAGER - HERRAMIENTAS DE CONTROL")
        print("="*50)
        print("1. Escuchar mensajes SNS")
        print("2. Generar entradas en DynamoDB desde bucket S3")
        print("3. Resetear estados 'in_process' de ocr_done a 'false'")
        print("4. Resetear estados 'in_process' de odoo_loaded a 'false'")
        print("5. Listar colas SQS disponibles")
        print("6. Enviar mensaje de prueba a SNS")
        print("7. 🗑️  Eliminar contenido del bucket S3")
        print("8. 🗑️  Eliminar contenido de la tabla DynamoDB")
        print("9. 📥 Sincronizar PDFs desde Odoo a S3 y DynamoDB")
        print("10. Salir")
        print("-"*50)
        
        choice = input("Selecciona una opción (1-10): ").strip()
        
        if choice == '1':
            manager.listen_sns_messages()
                
        elif choice == '2':
            prefix = input("Ingresa el prefijo/carpeta (opcional, presiona Enter para todo el bucket): ").strip()
                
            print(f"\nBuscando PDFs en s3://{manager.bucket_name}/{prefix}")
            
            confirm = input("¿Continuar? (y/N): ").strip().lower()
            if confirm == 'y':
                manager.generate_dynamodb_entries(prefix)
            else:
                print("Operación cancelada")
                
        elif choice == '3':
            confirm = input("¿Resetear todos los estados 'in_process' de ocr_done a 'false'? (y/N): ").strip().lower()
            if confirm == 'y':
                manager.reset_ocr_done_status()
            else:
                print("Operación cancelada")
                
        elif choice == '4':
            confirm = input("¿Resetear todos los estados 'in_process' de odoo_loaded a 'false'? (y/N): ").strip().lower()
            if confirm == 'y':
                manager.reset_odoo_loaded_status()
            else:
                print("Operación cancelada")
        
        elif choice == '5':
            manager.list_sqs_queues()
        
        elif choice == '6':
            manager.send_test_message()
        
        elif choice == '7':
            manager.delete_bucket_contents()
        
        elif choice == '8':
            manager.delete_dynamodb_table_contents()
        
        elif choice == '9':
            print("\n📥 SINCRONIZACIÓN ODOO → S3 → DYNAMODB")
            print("Esta operación:")
            print("- Descarga PDFs desde Odoo")
            print("- Los sube a S3 con estructura de carpetas")
            print("- Crea entradas en DynamoDB para procesamiento OCR")
            print("- Salta archivos que ya existen en S3")
            
            mode = input("\n¿Procesar paso a paso (p) o continuo (c)? [p/c]: ").strip().lower()
            step_by_step = mode != 'c'
            
            confirm = input(f"¿Continuar con modo {'paso a paso' if step_by_step else 'continuo'}? (y/N): ").strip().lower()
            if confirm == 'y':
                manager.sync_odoo_to_s3_and_dynamodb(step_by_step)
            else:
                print("Operación cancelada")
                
        elif choice == '10':
            print("¡Hasta luego!")
            break
            
        else:
            print("Opción no válida. Por favor selecciona 1-10.")


if __name__ == "__main__":
    main()
