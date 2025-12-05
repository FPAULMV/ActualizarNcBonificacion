import sys
from ftplib import FTP
from pathlib import Path
import paramiko
import socket


class Sftp():
    def __init__(self):
        pass

    def ftp_send_file(self, host: str, port: int, user: str, psw: str, local_file: Path) -> None:
        
        if local_file.exists():
            with FTP() as ftp:
                #print(f"Enviando documento: {local_file.name}")
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

            registros = {
                "exitosos": [],
                "fallidos": []
            } 
        
            print(f"Archivos a enviar: {len(files)}")
            for local_file in files:
                if local_file is None:
                    print(f"[ERROR] la lista incluye un elemento no valido -> {local_file}")
                    registros['fallidos'].append(str(f"Archivo invalido->{local_file}"))
                    continue

                if not local_file.exists():
                    print(f"[ERROR] No se encontró: {local_file}")
                    registros['fallidos'].append(local_file)
                    continue

                print(f"Enviando: {local_file.name}")
                with open(local_file, "rb") as f:
                    ftp.storbinary(f"STOR {local_file.name}", f)
                    registros['exitosos'].append(local_file)
                
            print(f"Total de archivos enviados: {len(registros['exitosos'])}")
            print(f"Total de archivos NO enviados: {len(registros['fallidos'])}")
            if len(registros['fallidos']) > 0:
                print(f"DETALLE: {registros['fallidos']}")

    def ssh_send_list_files(self, host: str, port: int, user: str, psw: str, files: list[Path]) -> None:
        """Envía múltiples archivos por SSH."""
        try:
            sock = socket.create_connection((host, port), timeout=10)
            transport = paramiko.Transport(sock)
            transport.banner_timeout = 10
            transport.connect(username=user, password=psw)
            sftp = paramiko.SFTPClient.from_transport(transport)
        except paramiko.ssh_exception.AuthenticationException:
            print(f"[ERROR] Autenticación SSH fallida para usuario: {user}")
            print(f"[INFO] Verifica que la contraseña sea correcta: {psw}")
            return
        except Exception as e:
            print(f"[ERROR] No se pudo conectar por SSH: {e}")
            return

        registros = {
            "exitosos": [],
            "fallidos": []
        } 
    
        print(f"Archivos a enviar: {len(files)}")
        for local_file in files:
            if local_file is None:
                print(f"[ERROR] la lista incluye un elemento no valido -> {local_file}")
                registros['fallidos'].append(str(f"Archivo invalido->{local_file}"))
                continue

            if not local_file.exists():
                print(f"[ERROR] No se encontró: {local_file}")
                registros['fallidos'].append(local_file)
                continue

            print(f"Enviando: {local_file.name}")
            sftp.put(str(local_file), local_file.name)
            registros['exitosos'].append(local_file)
            
        sftp.close()
        transport.close()
        
        print(f"Total de archivos enviados: {len(registros['exitosos'])}")
        print(f"Total de archivos NO enviados: {len(registros['fallidos'])}")
        if len(registros['fallidos']) > 0:
            print(f"DETALLE: {registros['fallidos']}")