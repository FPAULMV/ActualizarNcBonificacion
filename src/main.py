import versioning, os, urllib.parse, sys, pandas, time, servicio_sftp, shutil
from pathlib import Path
from sqlalchemy import create_engine, text
from pandas import DataFrame
from dotenv import load_dotenv
from datetime import datetime, timedelta


class Bonificaciones():
    def __init__(self):
        pass

    def obtener_vencimientos(self, query: str) -> DataFrame:
        try:
            query = text(query)
            with engine.connect() as conn:
                df = pandas.read_sql(query, conn)
                if df.empty:
                    print("La consulta de vencimientos devolvio un DataFrame vacio.")
                    sys.exit("-> Fin de la ejecucion del programa. <-".upper())
                
                df_filtrado = self.filtrar_df(df)
                df_formateado = self.formatear_fechas_df(df_filtrado)
                return df_formateado
            
        except Exception as e:
            print("ERROR: Hubo un error al consultar los vencimientos.")
            print(f"DETALLE: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino la consulta de los vencimientos.\n")

    def filtrar_df(self, dataframe: DataFrame) -> DataFrame:
        """ Filtra el dataframe por 'Nota de Credito' """
        try:
            if dataframe.empty:
                print("El dataframe a filtrar está vacio.")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())
            
            df = dataframe[
                dataframe['Nota de Credito'].notnull() &
                dataframe["Nota de Credito"].str.contains("NC", na=False)
            ].copy()
            return df   
        except Exception as e:
            print("ERROR: Hubo un error al filtra el dataframe por 'Nota de Credito'.")
            print(f"DETALLE: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de filtra el dataframe por 'Nota de Credito'\n")

    def formatear_fechas_df(self, dataframe: DataFrame) -> DataFrame:
        """ Formatea las columnas 'Emision Sinergia' del dataframe."""
        
        try:
            if dataframe.empty:
                print("El dataframe a formatear está vacio.")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())

            df = dataframe
            df["Emision Sinergia"] = df["Emision Sinergia"].dt.strftime("%d-%m-%Y")
            return df
        except Exception as e:
            print("ERROR: Hubo un error al formatear la columna 'Emision Sinergia'.")
            print(f"DETALLE: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de filtra el dataframe por 'Nota de Credito'\n")

    def obtener_dataframe_portal(self, query: str) -> DataFrame:
        """Obtiene el dataframe con la informacion de Portal.ClientUser"""
        try:
            query = text(query)
            with engine.connect() as conn:
                df = pandas.read_sql(query, conn)
                if df.empty:
                    print("La consulta de portal.id devolvio un DataFrame vacio.")
                    sys.exit("-> Fin de la ejecucion del programa. <-".upper())
                    
                return df
        except Exception as e:
            print("ERROR: Hubo un error al consultar los portal.id.")
            print(f"DETALLE: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino la consulta de los portal.id.\n")

    def filtrar_columnas_dataframe(self, dataframe: DataFrame, columnas: list = None) -> DataFrame:
        try:
            if columnas is None:
                columnas = ["Nota de Credito", "Concepto", "Destino",
                            "Factura Sinergia", "Emision Sinergia",
                            "Total Aplicado", "Fecha Bonificada", "client_id"]
            
            self.comprobar_columnas(dataframe, columnas)

            dataframe_filtrado = dataframe[columnas]
            return dataframe_filtrado
        except Exception as e:
            print(f"ERROR: Hubo un error al obtener el dataframe filtrado.")
            print(f"DETALLES:\n{e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de filtrar el dataframe.\n")

    def comprobar_columnas(self, dataframe: DataFrame, columnas: list = None) -> DataFrame:
        """
            Comprueba que un DataFrame contenga todas las columnas nombradas
            en una lista.
        """
        valor = False
        try:
            campos_formateados = {str(c).upper().strip().replace(" ", "").replace("_", "") for c in columnas}
            df_columnas = {str(c).upper().strip().replace(" ", "").replace("_", "") for c in dataframe.columns}
            faltantes =  campos_formateados - df_columnas
            if faltantes:
                print(f"Faltan columnas en el DataFrame {sorted(faltantes)}")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())

            valor = True
            return valor
        except Exception as e:
            print("ERROR: Ocurrio un error al comprobar los campos del DataFrame")
            print(e)
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino la validacion de las columnas del DataFrame.\n")

    def obtener_clientes(self, data_frame: DataFrame) -> DataFrame:
        """
            Obtiene una lista con los clientes del DataFrame. 
            (O de lo que sea que incluya la columna 'Destinos.')
        """
        try:    
            clientes = data_frame[["Destino", "client_id"]].dropna().drop_duplicates(subset="Destino")
            if clientes.empty:
                print("INFO: No hay clientes para validar en el DataFrame.")
                sys.exit("-> Fin de la ejecucion del programa. <-".upper())
            return clientes
        except Exception as e:
            print("ERROR: Ocurrio un error al obtener la lista de clientes unicos.")
            print(e)
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de obtener la lista de los clientes unicos.\n")


if __name__ == '__main__':

    try:
        load_dotenv()
        QUERY_SINERGIA = os.getenv('QUERY_SINERGIA')
        QUERY_SINERGIA_PORTAL_ID = os.getenv('QUERY_SINERGIA_PORTAL_ID')
        
        # SQL
        CONN_STR = os.getenv('CONN_STR')
        odbc_encoded = urllib.parse.quote_plus(CONN_STR)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_encoded}",pool_pre_ping=True,fast_executemany=True)
        print("\n--- Variables de entorno cargadas con exito. ---\n".upper())
        
    except Exception as e:
        # Agregar Log.Error
        print("Hubo un error al cargar las variables de entorno.")
        print(e)
        sys.exit("-> Fin de la ejecucion del programa. <-".upper())
    finally:
        #Agregar Log.info
        print("-> FIN: de la carga de las variables de entorno.\n")


    bonificacion = Bonificaciones()
    sftp = servicio_sftp.Sftp()
    df_completo = bonificacion.obtener_vencimientos(QUERY_SINERGIA)
    df_portal = bonificacion.obtener_dataframe_portal(QUERY_SINERGIA_PORTAL_ID)
    clientes = bonificacion.obtener_clientes(df_completo) # Clientes unicos

    print(df_completo.to_string())
    print(df_portal)
    print(clientes)
