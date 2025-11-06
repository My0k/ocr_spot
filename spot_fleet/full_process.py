#!/usr/bin/env python3
import os
import sys
from get_pdf_from_s3 import PDFDownloader
from generate_ocr_layer import OCRProcessor
from upload_pdf_to_s3 import PDFUploader

class FullOCRProcessor:
    def __init__(self, config_file: str = 'config.conf'):
        """Inicializa el procesador completo OCR"""
        self.downloader = PDFDownloader(config_file)
        self.ocr_processor = OCRProcessor()
        self.uploader = PDFUploader(config_file)
    
    def is_historic_pdf(self, file_path: str) -> bool:
        """Verifica si el PDF es histórico basándose en el nombre del archivo"""
        filename = os.path.basename(file_path).lower()
        return 'historico' in filename
    
    def process_single_pdf(self, language: str = 'spa'):
        """Procesa un solo PDF completo: descarga -> OCR -> subida"""
        print("=== Iniciando proceso completo OCR ===")
        
        # Paso 1: Obtener PDF desde S3 y cambiar estado a in_process
        print("\n1. Obteniendo PDF desde S3...")
        pdf_info = self.downloader.process_next_pdf()
        
        if not pdf_info:
            print("❌ No hay PDFs disponibles para procesar")
            return False
        
        input_path = pdf_info['input_path']
        local_path = pdf_info['local_path']
        
        print(f"✅ PDF descargado: {local_path}")
        
        # Verificar si es archivo histórico
        is_historic = self.is_historic_pdf(local_path)
        if is_historic:
            print("📜 Archivo histórico detectado - se copiará sin OCR")
        
        try:
            # Paso 2: Procesar PDF con OCR o copiar si es histórico
            if is_historic:
                print("\n2. Copiando archivo histórico...")
                ocr_output_path, error_occurred = self.ocr_processor.apply_ocr(local_path, language)
            else:
                print("\n2. Aplicando OCR al PDF...")
                ocr_output_path, error_occurred = self.ocr_processor.apply_ocr(local_path, language)
            
            if not ocr_output_path or error_occurred:
                error_msg = "Error procesando archivo" if error_occurred else "Error desconocido"
                print(f"❌ {error_msg}")
                self.downloader.revert_status(input_path, error_occurred=error_occurred)
                self.cleanup_local_files(local_path)
                # Retornar None para indicar que hubo un error pero que se debe continuar
                # En lugar de False que indica "no hay más archivos"
                return None
            
            process_type = "copiado (histórico)" if is_historic else "OCR aplicado"
            print(f"✅ {process_type}: {ocr_output_path}")
            
            # Paso 3: Subir PDF procesado a S3 y cambiar estado a true
            print(f"\n3. Subiendo PDF {'histórico' if is_historic else 'procesado'} a S3...")
            output_s3_path = self.uploader.upload_pdf(ocr_output_path, input_path, is_historic)
            
            if not output_s3_path:
                print("❌ Error subiendo PDF a S3")
                self.cleanup_local_files(local_path, ocr_output_path)
                return None  # Error, pero continuar con siguiente archivo
            
            print(f"✅ PDF subido a S3: {output_s3_path}")
            
            # Paso 4: Limpiar archivos locales (tanto original como procesado)
            print("\n4. Limpiando archivos locales...")
            self.cleanup_local_files(local_path, ocr_output_path)
            
            print("\n=== Proceso completado exitosamente ===")
            print(f"Archivo original: {input_path}")
            print(f"Archivo procesado: {output_s3_path}")
            if is_historic:
                print("📜 Archivo histórico - copiado sin OCR")
            print("📁 Archivos locales eliminados para liberar espacio")
            
            return True
            
        except Exception as e:
            print(f"❌ Error durante el procesamiento: {e}")
            self.downloader.revert_status(input_path, error_occurred=True)
            self.cleanup_local_files(local_path, ocr_output_path if 'ocr_output_path' in locals() else None)
            return None  # Error, pero continuar con siguiente archivo
    
    def cleanup_local_files(self, *file_paths):
        """Limpia archivos locales"""
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"Archivo eliminado: {file_path}")
                except Exception as e:
                    print(f"Error eliminando {file_path}: {e}")
    
    def process_continuous(self, language: str = 'spa', max_iterations: int = None):
        """Procesa PDFs continuamente hasta que no haya más disponibles"""
        print("=== Iniciando procesamiento continuo ===")
        
        processed_count = 0
        iteration = 0
        
        while True:
            if max_iterations and iteration >= max_iterations:
                print(f"Límite de iteraciones alcanzado: {max_iterations}")
                break
            
            iteration += 1
            print(f"\n--- Iteración {iteration} ---")
            
            success = self.process_single_pdf(language)
            
            if success:
                processed_count += 1
                print(f"PDFs procesados hasta ahora: {processed_count}")
            else:
                print("No hay más PDFs para procesar o hubo un error")
                break
        
        print(f"\n=== Procesamiento continuo finalizado ===")
        print(f"Total de PDFs procesados: {processed_count}")
        
        return processed_count


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
    
    try:
        processor = FullOCRProcessor(args.config)
        
        if args.continuous:
            processed = processor.process_continuous(args.language, args.max_iterations)
            if processed > 0:
                sys.exit(0)
            else:
                sys.exit(1)
        else:
            success = processor.process_single_pdf(args.language)
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()