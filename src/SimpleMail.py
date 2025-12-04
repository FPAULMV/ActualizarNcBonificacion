import smtplib, re
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional
from datetime import datetime


class Correo:

    def __init__(
        self,
        servidor: str,
        puerto: int,
        usuario: str,
        contrasena: str,
        remitente: str = None,
        usar_tls: bool = True
    ):
        self.servidor = servidor
        self.puerto = puerto
        self.usuario = usuario
        self.contrasena = contrasena
        self.remitente = remitente or usuario
        self.usar_tls = usar_tls

    def _html_to_str(self, path_file: Path) -> str:
        """ Lee un html desde su ruta como Path y devuelve el contenido como str."""
        ruta = Path(path_file)
        if not ruta.exists():
            raise FileNotFoundError(f"El no se encuentra archivo html: {ruta.name}")
        
        return ruta.read_text(encoding='utf-8', errors='ignore')

    def enviar(
        self,
        to: List[str],
        cc: Optional[List[str]] = None,
        subject: str = "",
        ruta_html: Optional[Path] = None,
        cuerpo_texto: Optional[str] = None,
        adjuntos: Optional[List[Path]] = None
    ):
        print("inicio")

        msg = EmailMessage()
        msg["From"] = self.remitente
        msg["To"] = ", ".join(to)

        if cc:
            msg["Cc"] = ", ".join(cc)

        msg["Subject"] = subject

        # Cuerpo
        if ruta_html:
            cuerpo_html = self._html_to_str(ruta_html)
            msg.set_content(cuerpo_texto or "Tu cliente no soporta HTML.")
            msg.add_alternative(cuerpo_html, subtype="html")

        elif cuerpo_texto:
            msg.add_alternative(cuerpo_texto, subtype="html")

        else:
            msg.set_content(cuerpo_texto or "No hay cuerpo del correo para adjuntar.")

        # Adjuntos
        if adjuntos:
            for file in adjuntos:
                archivo = Path(file)
                if archivo.exists():
                    with open(archivo, "rb") as f:
                        msg.add_attachment(
                            f.read(),
                            maintype="application",
                            subtype="octet-stream",
                            filename=archivo.name
                        )

        print("Conectando...")

        # ---- IMPORTANTE ----
        if self.puerto == 465:
            smtp = smtplib.SMTP_SSL(self.servidor, self.puerto)
        else:
            smtp = smtplib.SMTP(self.servidor, self.puerto)
            if self.usar_tls:
                smtp.starttls()

        with smtp:
            smtp.login(self.usuario, self.contrasena)
            smtp.send_message(msg)

        print("Correo enviado correctamente")


class Servicios():
    def __init__(self, plantilla_html: Path, formato_marcador: str = '{{}}'):
        self.plantilla_html = plantilla_html
        self.formato_marcador = formato_marcador
        
        # Extraer el patrón de marcador
        # Ejemplo: '{{}}' -> '{{(.*?)}}'
        # Ejemplo: '[/]' -> '\[/(.*?)/\]'
        # Ejemplo: '$$' -> '\$\$(.*?)\$\$'
        mitad = len(formato_marcador) // 2
        inicio = re.escape(formato_marcador[:mitad])
        fin = re.escape(formato_marcador[mitad:])
        self.patron_regex = f"{inicio}(.*?){fin}"

    def format_html(self, reemplazos: dict[str, any] = {}) -> str:
        ruta = Path(self.plantilla_html)
        
        if not ruta.exists():
            raise FileNotFoundError(f"El no se encuentra archivo html: {ruta.name}")
        
        with open(self.plantilla_html, 'r', encoding='utf-8') as f:
            html = f.read()
        
        # Primero, buscar todos los marcadores en el HTML
        marcadores_encontrados = re.findall(self.patron_regex, html)
        
        # Reemplazar cada marcador encontrado
        for marcador_nombre in marcadores_encontrados:
            # Crear el marcador completo con delimitadores
            marcador_completo = f"{self.formato_marcador[:len(self.formato_marcador)//2]}{marcador_nombre}{self.formato_marcador[len(self.formato_marcador)//2:]}"
            
            # Determinar el valor de reemplazo
            if marcador_nombre == "fecha_actual":
                valor_reemplazo = str(datetime.now())
            elif marcador_nombre in reemplazos:
                valor_reemplazo = str(reemplazos[marcador_nombre])
            else:
                # Si no hay reemplazo definido, dejar el marcador como está
                continue
            
            # Reemplazar TODAS las ocurrencias del marcador
            html = html.replace(marcador_completo, valor_reemplazo)
        
        return html