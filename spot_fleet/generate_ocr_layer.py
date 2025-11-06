#!/usr/bin/env python3
import os
import sys
import ocrmypdf

class OCRProcessor:
    def __init__(self):
        """Inicializa el procesador OCR"""
        self.output_dir = 'pdfs_with_ocr'
        os.makedirs(self.output_dir, exist_ok=True)

    def is_historic_pdf(self, input_pdf_path: str) -> bool:
        """Verifica si el PDF es histórico basándose en el nombre del archivo"""
        filename = os.path.basename(input_pdf_path).lower()
        return 'historico' in filename

    def apply_ocr(self, input_pdf_path: str, language: str = 'spa'):
        """Aplica OCR a un PDF usando OCRmyPDF o copia si es histórico"""
        if not os.path.exists(input_pdf_path):
            print(f"Error: No se encontró el archivo {input_pdf_path}")
            return None, True  # Retorna error=True

        input_filename = os.path.basename(input_pdf_path)
        name, ext = os.path.splitext(input_filename)
        output_filename = f"{name}_ocr{ext}"
        output_path = os.path.join(self.output_dir, output_filename)

        # Verificar si es archivo histórico
        if self.is_historic_pdf(input_pdf_path):
            print(f"📜 Archivo histórico detectado: {input_pdf_path}")
            print("Copiando archivo sin aplicar OCR...")
            try:
                import shutil
                shutil.copy2(input_pdf_path, output_path)
                print(f"Archivo histórico copiado: {output_path}")
                return output_path, False  # Sin error
            except Exception as e:
                print(f"Error copiando archivo histórico: {e}")
                return None, True  # Error

        print(f"Procesando OCR → {input_pdf_path}")
        try:
            ocrmypdf.ocr(
                input_file=input_pdf_path,
                output_file=output_path,
                language=language,
                deskew=True,
                rotate_pages=True,
                remove_background=False,
                optimize=1,
                pdf_renderer='auto',
                force_ocr=False,
                skip_text=True,
                redo_ocr=False,
                keep_temporary_files=False,
                progress_bar=False
            )
            print(f"OCR generado: {output_path}")
            return output_path, False  # Sin error

        except ocrmypdf.exceptions.PriorOcrFoundError:
            print("El PDF ya contiene texto OCR, copiando archivo...")
            import shutil
            shutil.copy2(input_pdf_path, output_path)
            print(f"Archivo copiado (ya tiene OCR): {output_path}")
            return output_path, False  # Sin error

        except ocrmypdf.exceptions.TaggedPDFError:
            print("El PDF es un Tagged PDF (ya tiene texto), copiando archivo...")
            import shutil
            shutil.copy2(input_pdf_path, output_path)
            print(f"Archivo copiado (Tagged PDF): {output_path}")
            return output_path, False  # Sin error

        except ocrmypdf.exceptions.EncryptedPdfError:
            print("❌ Error: El PDF está encriptado y no se puede procesar")
            return None, True  # Error crítico

        except Exception as e:
            # Si el error contiene "Tagged PDF", también lo manejamos
            if "Tagged PDF" in str(e):
                print("El PDF es un Tagged PDF (ya tiene texto), copiando archivo...")
                import shutil
                try:
                    shutil.copy2(input_pdf_path, output_path)
                    print(f"Archivo copiado (Tagged PDF): {output_path}")
                    return output_path, False  # Sin error
                except Exception as copy_error:
                    print(f"Error copiando Tagged PDF: {copy_error}")
                    return None, True  # Error
            
            # Verificar si es un error de PDF corrupto
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['corrupt', 'damaged', 'invalid pdf', 'malformed']):
                print(f"❌ Error: PDF corrupto o dañado - {e}")
                return None, True  # Error crítico
            
            print(f"❌ Error aplicando OCR: {e}")
            return None, True  # Error crítico


def main():
    if len(sys.argv) < 2:
        print("Uso: python generate_ocr_layer.py <ruta_pdf_input> [idioma]")
        sys.exit(1)

    input_pdf = sys.argv[1]
    language = sys.argv[2] if len(sys.argv) > 2 else 'spa'

    processor = OCRProcessor()
    output_path, error_occurred = processor.apply_ocr(input_pdf, language)

    if output_path and not error_occurred:
        print(f"✅ OCR completado: {output_path}")
    else:
        print("❌ Error: No se pudo generar el PDF con OCR")
        sys.exit(1)


if __name__ == "__main__":
    main()
