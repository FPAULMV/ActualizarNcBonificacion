__app_name__ = "Vencimientos" 
__version__ = "1.0.0"
__release_date__ = "2025-11-05"
__author__ = "© Paul Morales -> paul.morales@fpaulmv.com"

class Version():
    def __init__(self):
        self.app_version()

    def app_version(self) -> None:
        """Imprime la informacion de la aplicacion en consola."""

        banner = "="*50

        print(banner)
        print(f"    --- {__app_name__} ---  \n")
        print(f"Última versión: {__version__} | Actualizado: {__release_date__}.\n")
        print(f"       {__author__}")
        print(banner)
        print("\nIniciando...\n")    

vesion = Version()