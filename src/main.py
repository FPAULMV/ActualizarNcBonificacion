import versioning, os, urllib.parse, sys, pandas, time, servicio_sftp, shutil
from pathlib import Path
from sqlalchemy import create_engine, text
from pandas import DataFrame
from dotenv import load_dotenv
from datetime import datetime, timedelta


class Bonificaciones():
    def __init__(self):
        pass

    def obtener_ncbonificacion(self, query: str) -> DataFrame:
        try:
            query = text(query)
            with engine.connect() as conn:
                df = pandas.read_sql(query, conn)
                if df.empty:
                    print("La consulta de vencimientos devolvio un DataFrame vacio.")
                    sys.exit("-> Fin de la ejecucion del programa. <-".upper())
                
                df_filtrado = self.filtrar_df(df)
                formato_int = self.formatear_columna(df_filtrado, 'client_id', 'NUMERO')
                df_formateado = self.formatear_fechas_df(formato_int)
                return df_formateado
            
        except Exception as e:
            print("ERROR: Hubo un error al consultar el Concentrado de NC.")
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
    

    def formatear_columna(self, dataframe: DataFrame, columna: str, formato: str = None ) -> DataFrame:
        """ 
            Cambia el formato de una columna a un formato aplicable.
            Formatos -> 'TEXTO, NUMERO, FECHA, FLOAT'
        """

        if columna not in dataframe.columns:
            raise ValueError(f"La coumna '{columna}' no se encuentra en el dataframe.")
        
        serie = dataframe[columna]

        if formato is None:
            return dataframe
        
        f = formato.upper()
        
        if f == 'TEXTO':
            dataframe[columna] = serie.astype(str)
            return dataframe
        
        elif f == 'NUMERO':
            dataframe[columna] = pandas.to_numeric(serie, errors='coerce').astype('Int64')
            return dataframe
        
        elif f == 'FECHA':
            dataframe[columna] = pandas.to_datetime(serie, errors='coerce', dayfirst=True)
            return dataframe
        
        elif f == 'FLOAT':
            dataframe[columna] = pandas.to_numeric(serie, errors='coerce')
            return dataframe

        else:
            formatos_validos = 'TEXTO, NUMERO, FECHA, FLOAT'
            raise ValueError(f"El formato '{f}' no es un formato soportado, pruebe con uno de los siguientes: {formatos_validos}")

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

    def obtener_portal_id(self, client_id: str, dataframe_portal: DataFrame) -> list[str]:
        """Filtra el dataframe con la informacion del portal y obtiene el id del cliente buscado."""
        try:
            df = dataframe_portal.copy()
            df['ClientId'] = df['ClientId'].astype(str).str.strip()
            client_id = str(client_id).strip()
            if client_id in df["ClientId"].unique():
                valor_id = df.loc[df["ClientId"] == client_id, "Id"].iloc[0]
                return valor_id
            else:
                return None
        except Exception as e:
            print(f"ERROR: Ocurrio un error al buscar el Id del cliente {client_id}")
            print(e)

    def obtener_dataframe_cliente(self, dataframe: DataFrame, cliente: str) -> DataFrame:
        """Filtra un dataframe segun el cliente que se le indique."""
        try:
            df_cliente = dataframe[dataframe['Destino'] == cliente]
            return df_cliente
        except Exception as e:
            print("ERROR: Ocurrio un error al filtrar el dataframe segun el cliente.")
            print(f"DETALLES:\n{e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print(f"-> FIN: Termino de obtener el dataframe segun el cliente.\n")

    def crear_cardsystem_insert(self, 
            nombre_mostrar : str,
            nombre_archivo: str,
            destino: str) -> dict:
        """ Crea un diccionario, util para hacer un INSERT a Cardsystem.ArchivosGenerales"""
        
        try:
            ahora = datetime.now()
            fecha_actual = ahora.strftime('%Y-%m-%d %H:%M:%S')
            fecha_final = (ahora + timedelta(days= 2)).strftime('%Y-%m-%d %H:%M:%S')

            return {
                "nombre": nombre_mostrar,
                "nombre_archivo": nombre_archivo.replace("/", "").replace("\\", ""),
                "destino": str(destino),
                "tipo": 1,
                "fecha_inicio": fecha_actual,
                "fecha_fin": fecha_final,
                "fecha_creacion": fecha_actual,
                }
        except Exception as e:
            print("ERROR: Ocurrio un error al crear la informacion para cardsystem.")
            print(f"DETALLES:\n{e}")

    def delete_registro_nc(self) -> None:
        """Elimina todos los registros de 'Notas de crédito bonificacion' en CardSystem.ArchivosGenerales"""
        print("Eliminando 'Notas de crédito bonificacion'.")
        try: 
            with engine.begin() as conn:
                query = text("""DELETE FROM [NexusFuel].[CardSystem].[ArchivosGenerales]
                             WHERE [Nombre] = 'Notas de crédito bonificación'""")
                conn.execute(query)
        except Exception as e:
            print(f"ERROR: Ocurrio un error al eliminar los registros de 'Notas de crédito bonificacion' en CardSystem.ArchivosGenerales.")
            print(f"DETALLES:\n{e}")
        finally:
            print(f"-> FIN: Termino la eliminacion de 'Notas de crédito bonificacion' en CardSystem.ArchivosGenerales.")


    def excect_cardsystem_delete(self, delete_values: list[dict]) -> None:
        """ Ejecuta una transaccion donde elimina multiples registros de CardSystem.ArchivosGenerales. """

        delete_cardsystem = text("""
            DELETE FROM [NexusFuel].[CardSystem].[ArchivosGenerales]
            WHERE nombreArchivo = :nombre_archivo;
            """)
        
        registros = {
            "exitosos": [],
            "fallidos": [],
            "errores": []
        }

        print(f"Total de registros a eliminar: {len(delete_values)}")
        with engine.connect() as conn:
            transaction = conn.begin()

            try: 
                for delete_value in delete_values:
                    try:
                        params = {'nombre_archivo': delete_value}
                        result = conn.execute(delete_cardsystem, params)
                        registros['exitosos'].append({
                            "nombre_archivo": delete_value,
                            "estado": "Elminado con exito.",
                            "error": "NA"
                        })
                    except Exception as e:
                        registros['fallidos'].append({
                            "nombre_archivo": delete_value,
                            "estado": "Error al eliminar.",
                            "error": str(e)
                        })
                        print(f"ERROR: Hubo un error al eliminar el registro: {delete_value}")
                        print(f"DETALLES:\n{e}")
                transaction.commit()
                print(f"Registros eliminados correctamente.")
            except Exception as e:
                transaction.rollback()
                print(f"INFO: Transaccion delete revertida. {str(e)}")
                registros["errores"].append({
                            "error_general": str(e)
                        })
        return registros

    def excect_cardsystem_insert(self, insert_values: list[dict]) -> None:
        """ Ejecuta una transaccion de multiples insert a cardsystem. """

        insert_cardsystem = text("""
            INSERT INTO [NexusFuel].[CardSystem].[ArchivosGenerales] 
            (Nombre, nombreArchivo, Destino, Tipo, FechaInicio, FechaFin, FechaCreacion)
            VALUES (:nombre, :nombre_archivo, :destino, :tipo, :fecha_inicio, :fecha_fin, :fecha_creacion);
            """)
        
        registros = {
            "exitosos": [],
            "fallidos": [],
            "errores": []
        }

        print(f"Total de registros a insertar: {len(insert_values)}")
        with engine.connect() as conn:
            transaction = conn.begin()

            try:
                for insert in insert_values:
                    try:
                        result = conn.execute(insert_cardsystem, insert)
                        registros['exitosos'].append({
                            "datos": insert,
                            "nombre_archivo": insert['nombre_archivo'],
                            "destino": insert['destino']
                            })
                    except Exception as e:
                        registros["fallidos"].append({
                                "datos": insert,
                                "nombre_archivo": insert["nombre_archivo"],
                                "destino": insert['destino'],
                                "error": str(e)
                            })
                        print(f"ERROR: Hubo un error al insertar el registro: {insert['nombre_archivo']}")
                        print(f"DETALLES:\n{e}")
                transaction.commit()
                print(f"Registros guardados correctamente.")
            except Exception as e:
                transaction.rollback()
                print(f"INFO: Transaccion insert revertida. {str(e)}")
                registros["errores"].append(f"Error general: {str(e)}")
        return registros
    
    def datos_del_cliente(self, dataframe: DataFrame, nombre_referencia: str) -> dict:
        """ Obtiene los requeridos datos del dataframe que filtramos por cliente"""
        try:
            nombre_cliente = dataframe['Destino'].unique()[0]
            nombre_archivo = str(f"{nombre_referencia} {nombre_cliente}.csv")
            client_id = dataframe['client_id'].unique()[0]
            return {
                "nombre_cliente": nombre_cliente,
                "nombre_archivo": nombre_archivo,
                "client_id": str(client_id)
            }
        except Exception as e:
            print(f"ERROR: Hubo un error al crear el diccionario con los datos del cliente.")
            print(f"DETALLES:\n{e}")
        finally:
            print("-> FIN: Termino de crear el diccionario con los datos del cliente.\n")

class CrearDoumento():
    """Crea documentos a partir de un DataFrame."""

    def __init__(self, data_frame: DataFrame, carpeta_salida: Path):
        self.data_frame = data_frame
        self.carpeta_salida = carpeta_salida

        try:
            self.carpeta_salida.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"ERROR al crear la carpeta: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())

    def crear_documentos_csv(self, nombre_archivo: str) -> Path:
        """ Crea un documento '.csv' """
        try:
            nombre_formateado = nombre_archivo.replace("/", "").replace("\\", "")
            ruta = Path(self.carpeta_salida / nombre_formateado)
            self.data_frame.to_csv(ruta, index= False, encoding= 'utf-8')
            return ruta
        except Exception as e:
            print(f"Hubo un error al crear el archivo. {ruta}")
            print(e)
        finally:
            print("-> FIN: Termino la creacion del archivo.\n")
    
    def limpiar_archivos(self, folder: Path) -> None:
        """Borra solo archivos dentro de una carpeta (sin tocar subcarpetas)."""

        if not folder.exists() or not folder.is_dir():
            print(f"[ERROR] Carpeta inválida: {folder}")
            return

        for file in folder.glob("*"):
            if file.is_file():
                try:
                    file.unlink()
                except Exception as e:
                    print(f"[ERROR] No se pudo borrar {file}: {e}")


if __name__ == '__main__':

    try:
        load_dotenv()
        # SFTP
        HOST = str(os.getenv('HOST'))
        PORT = int(os.getenv('PORT'))
        USER = str(os.getenv('USER'))
        PSW = str(os.getenv('PSW'))

        # PATS
        PATH_FILES_SINERGIA = Path(os.getenv('PATH_FILES_SINERGIA'))
        
        # SQL
        QUERY_SINERGIA = os.getenv('QUERY_SINERGIA')
        QUERY_SINERGIA_PORTAL_ID = os.getenv('QUERY_SINERGIA_PORTAL_ID')
        CONN_STR = urllib.parse.quote_plus(os.getenv('CONN_STR'))
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}",pool_pre_ping=True,fast_executemany=True)
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
    df_completo = bonificacion.obtener_ncbonificacion(QUERY_SINERGIA)
    df_portal = bonificacion.obtener_dataframe_portal(QUERY_SINERGIA_PORTAL_ID)
    clientes = bonificacion.obtener_clientes(df_completo)


    CLIENTES_SIN_ID = []
    ARCHIVOS_CREADOS_PATHS = []
    INSERTS_ARCHIVOS_GENERALES = []
    CLIENTES_NO_ENCONTRADOS = []
    DELETE_ARCHIVOS_GENERALES = []

    for _, row in clientes.iterrows():
        cliente = row['Destino']
        client_id = row['client_id']
        id_cliente = bonificacion.obtener_portal_id(client_id, df_portal)
        if id_cliente is None:
            CLIENTES_SIN_ID.append(id_cliente)
            continue
        if cliente in df_completo['Destino'].values:
            df_unico_cliente = bonificacion.obtener_dataframe_cliente(df_completo, cliente)
            datos_cliente = bonificacion.datos_del_cliente(df_unico_cliente, "NC")
            columnas_requeridas = ["Nota de Credito", "Concepto", "Destino",
                            "Factura Sinergia", "Emision Sinergia",
                            "Total Aplicado", "Fecha Bonificada"]

            df_salida = bonificacion.filtrar_columnas_dataframe(df_unico_cliente, columnas_requeridas)
            
            archivo_salida = CrearDoumento(df_salida, PATH_FILES_SINERGIA)
            ruta_salida = archivo_salida.crear_documentos_csv(datos_cliente['nombre_archivo'])
            ARCHIVOS_CREADOS_PATHS.append(ruta_salida)
            INSERTS_ARCHIVOS_GENERALES.append(bonificacion.crear_cardsystem_insert("Notas de crédito bonificación",datos_cliente['nombre_archivo'],id_cliente))
            DELETE_ARCHIVOS_GENERALES.append(datos_cliente['nombre_archivo'])
        
        else:
            CLIENTES_NO_ENCONTRADOS.append(cliente)

    bonificacion.excect_cardsystem_delete(DELETE_ARCHIVOS_GENERALES)
    registros_insert = bonificacion.excect_cardsystem_insert(INSERTS_ARCHIVOS_GENERALES)
    sftp.ftp_send_list_files(HOST, PORT, USER, PSW, ARCHIVOS_CREADOS_PATHS)
    archivo_salida.limpiar_archivos(PATH_FILES_SINERGIA)
    print(("-> Fin de la ejecucion del programa. <-".upper()))




