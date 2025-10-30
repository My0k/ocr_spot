#!/usr/bin/env python3
import os
import sys
import ocrmypdf

class OCRProcessor:
    def __init__(self):
        """Inicializa el procesador OCR"""
        self.output_dir = 'pdfs_with_ocr'
        os.makedirs(self.output_dir, exist_ok=True)

    def apply_ocr(self, input_pdf_path: str, language: str = 'spa'):
        """Aplica OCR a un PDF usando OCRmyPDF"""
        if not os.path.exists(input_pdf_path):
            print(f"Error: No se encontró el archivo {input_pdf_path}")
            return None

        input_filename = os.path.basename(input_pdf_path)
        name, ext = os.path.splitext(input_filename)
        output_filename = f"{name}_ocr{ext}"
        output_path = os.path.join(self.output_dir, output_filename)

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
                skip_text=True,  # Cambiar a True para saltar texto existente
                redo_ocr=False,
                keep_temporary_files=False,
                progress_bar=False
            )
            print(f"OCR generado: {output_path}")
            return output_path

        except ocrmypdf.exceptions.PriorOcrFoundError:
            print("El PDF ya contiene texto OCR, copiando archivo...")
            import shutil
            shutil.copy2(input_pdf_path, output_path)
            print(f"Archivo copiado (ya tiene OCR): {output_path}")
            return output_path

        except ocrmypdf.exceptions.TaggedPDFError:
            print("El PDF es un Tagged PDF (ya tiene texto), copiando archivo...")
            import shutil
            shutil.copy2(input_pdf_path, output_path)
            print(f"Archivo copiado (Tagged PDF): {output_path}")
            return output_path

        except ocrmypdf.exceptions.EncryptedPdfError:
            print("Error: El PDF está encriptado y no se puede procesar")
            return None

        except Exception as e:
            # Si el error contiene "Tagged PDF", también lo manejamos
            if "Tagged PDF" in str(e):
                print("El PDF es un Tagged PDF (ya tiene texto), copiando archivo...")
                import shutil
                shutil.copy2(input_pdf_path, output_path)
                print(f"Archivo copiado (Tagged PDF): {output_path}")
                return output_path
            
            print(f"Error aplicando OCR: {e}")
            return None


def main():
    if len(sys.argv) < 2:
        print("Uso: python generate_ocr_layer.py <ruta_pdf_input> [idioma]")
        sys.exit(1)

    input_pdf = sys.argv[1]
    language = sys.argv[2] if len(sys.argv) > 2 else 'spa'

    processor = OCRProcessor()
    output_path = processor.apply_ocr(input_pdf, language)

    if output_path:
        print(f"✅ OCR completado: {output_path}")
    else:
        print("❌ Error: No se pudo generar el PDF con OCR")


if __name__ == "__main__":
    main()
