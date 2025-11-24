import sys
from ftplib import FTP
from pathlib import Path


class Sftp():
    def __init__(self):
        pass

    def ftp_send_file(self, host: str, port: int, user: str, psw: str, local_file: Path) -> None:
        
        if local_file.exists():
            with FTP() as ftp:
                print(f"Enviando documento: {local_file.name}")
                ftp.connect(host, port, timeout=10)
                ftp.login(user, psw)

                # Subir el archivo.
                with open(local_file, "rb") as f:
                    ftp.storbinary(f"STOR {local_file.name}", f)
        else: 
            print(f"No se encontro la ruta al archivo: {local_file}")


    def ftp_send_list_files(self, host: str, port: int, user: str, psw: str, files: list[Path]) -> None:
        """Envía múltiples archivos por FTP."""
        with FTP() as ftp:
            ftp.connect(host, port, timeout=10)
            ftp.login(user, psw)

            for local_file in files:
                if not local_file.exists():
                    print(f"[ERROR] No se encontró: {local_file}")
                    continue

                print(f"Enviando: {local_file.name}")

                with open(local_file, "rb") as f:
                    ftp.storbinary(f"STOR {local_file.name}", f)
            


    def ftp_unlink(local_file: Path) -> None:
        """ Eliminar archivos. (Sin implementar. Sin uso especifico.)"""
        #local_file.unlink(missing_ok = False)
        pass
