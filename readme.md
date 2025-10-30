ocr_spot

sistema para generar ocr en pdfs masivamente desde spot instances de aws de forma paralela coordinado por dynamodb, obtenidos desde odoo y posteriormente subirlos a s3

main_tools.py: Herramientas para controlar y monitorear

/spot_fleet: carpeta que irá en la AMI de las instancias spot

config.conf: contiene las credenciales


ejemplo tabla dynamodb:

input_path	output_path	ocr_done	odoo_loaded
ejemplo/path/ens3	ejemplo/path/salidas3	true/false/in_process	true/false/in_process

in_process significa que otra instancia lo está procesando, por lo que no debe usarse aún