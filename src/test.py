import versioning, os, urllib.parse, sys, pandas, time, servicio_sftp, shutil
from pathlib import Path
from sqlalchemy import create_engine, text
from pandas import DataFrame
from dotenv import load_dotenv
from datetime import datetime, timedelta


class Vencimientos():
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
                    
                return df
        except Exception as e:
            print("ERROR: Hubo un error al consultar los vencimientos.")
            print(f"DETALLE: {e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino la consulta de los vencimientos.\n")


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

    def filtrar_columnas_dataframe(self, dataframe: DataFrame, columnas: list = None) -> DataFrame:
        try:
            if columnas is None:
                columnas = ["Destino", "NumeroFactura", "FechaFactura",
                            "Vencimiento", "total", "Saldo",
                            "ReferenciaBancaria", "dias_credito", "dias_vencido",
                            "client_id"]
            
            self.comprobar_columnas(dataframe, columnas)

            dataframe_filtrado = dataframe[columnas]
            return dataframe_filtrado
        except Exception as e:
            print(f"ERROR: Hubo un error al obtener el dataframe filtrado.")
            print(f"DETALLES:\n{e}")
            sys.exit("-> Fin de la ejecucion del programa. <-".upper())
        finally:
            print("-> FIN: Termino de filtrar el dataframe.\n")

    def clientes(self, data_frame: DataFrame) -> DataFrame:
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


    def datos_del_cliente(self, dataframe: DataFrame, nombre_referencia: str = "Vencimientos") -> dict:
        """ Obtiene los requeridos datos del dataframe que filtramos por cliente"""
        try:
            nombre_cliente = dataframe['Destino'].unique()
            nombre_archivo = str(f"{nombre_referencia} {nombre_cliente[0]}.csv")
            client_id = df_cliente['client_id'].unique()
            return {
                "nombre_cliente": nombre_cliente,
                "nombre_archivo": nombre_archivo,
                "client_id": client_id
            }
        except Exception as e:
            print(f"ERROR: Hubo un error al crear el diccionario con los datos del cliente.")
            print(f"DETALLES:\n{e}")
        finally:
            print("-> FIN: Termino de crear el diccionario con los datos del cliente.\n")

    def crear_cardsystem_insert(self, 
            nombre_mostrar : str,
            nombre_archivo: str,
            destino: str) -> dict:
        """ Crea un diccionario, util para hacer un INSERT de Sql"""
        
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
                        #print(f"INFO: Registro insertado: {insert['nombre_archivo']}")

                    except Exception as e:
                        registros["fallidos"].append(
                            {
                                "datos": insert,
                                "nombre_archivo": insert["nombre_archivo"],
                                "destino": insert['destino'],
                                "error": str(e)
                            })
                        print(f"ERROR: Hubo un error al insertar el registro: {insert['nombre_archivo']}")
                        print(f"DETALLES:\n{e}")
                
                transaction.commit()
                print(f"Registros guardados correctamente.")
                time.sleep(2)
            except Exception as e:
                transaction.rollback()
                print(f"INFO: Transaccion revertida. {str(e)}")
                registros["errores"].append(f"Error general: {str(e)}")
        return registros


    def delete_vencimientos(self) -> None:
        """Elimina todos los registros de Vencimientos en CardSystem.ArchivosGenerales"""
        print("Eliminando Vencimientos.")
        try: 
            with engine.begin() as conn:
                query = text("""DELETE FROM [NexusFuel].[CardSystem].[ArchivosGenerales]
                             WHERE [Nombre] = 'Vencimientos'""")
                conn.execute(query)
        except Exception as e:
            print(f"ERROR: Ocurrio un error al eliminar los registros de Vencimientos.")
            print(f"DETALLES:\n{e}")
        finally:
            print(f"-> FIN: Termino la eliminacion de vencimientos.")


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


class CrearDoumento():
    """Crea documentos a partir de un DataFrame."""

    def __init__(self, data_frame: DataFrame, carpeta_salida: Path):
        self.data_frame = data_frame
        self.carpeta_salida = Path(carpeta_salida)

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
            (print("-> FIN: Termino la creacion del archivo.\n"))
    
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
        PATH_FILES_SINERGIA = Path(os.getenv('PATH_FILES_SINERGIA'))
        QUERY_SINERGIA = os.getenv('QUERY_SINERGIA')
        QUERY_SINERGIA_PORTAL_ID = os.getenv('QUERY_SINERGIA_PORTAL_ID')
        PATH_FILES_PETRODIESEL = Path(os.getenv('PATH_FILES_PETRODIESEL'))
        PATH_FILES_PROENERGETICS = Path(os.getenv('PATH_FILES_PROENERGETICS'))
        CONN_STR = os.getenv('CONN_STR')
        HOST = str(os.getenv('HOST'))
        PORT = int(os.getenv('PORT'))
        USER = str(os.getenv('USER'))
        PSW = str(os.getenv('PSW'))
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

    sinergia = Vencimientos()
    sftp = servicio_sftp.Sftp()
    df_completo = sinergia.obtener_vencimientos(QUERY_SINERGIA)
    df_portal = sinergia.obtener_dataframe_portal(QUERY_SINERGIA_PORTAL_ID)
    df_vencimientos = sinergia.filtrar_columnas_dataframe(df_completo)
    clientes = sinergia.clientes(df_vencimientos) # Clientes unicos


    CLIENTES_NO_ENCONTRADOS = []
    INSERTS_ARCHIVOS_GENERALES = []
    CLIENTES_SIN_ID = []
    ARCHIVOS_CREADOS_PATHS = []


    for _, row in clientes.iterrows():
        cliente = row['Destino']
        client_id = row['client_id']
        # Aqui agregamos la logica para obtener el id del portal.
        id_cliente = sinergia.obtener_portal_id(client_id, df_portal)
        if id_cliente is None:
            CLIENTES_SIN_ID.append(client_id)
            print(f"El cliente {client_id} no se encuentra en Portal.ClienUser.")
            continue

        if cliente in df_vencimientos['Destino'].values:
            df_cliente = sinergia.obtener_dataframe_cliente(df_vencimientos, cliente)
            datos_cliente = sinergia.datos_del_cliente(df_cliente)
            df_a_mostrar = sinergia.filtrar_columnas_dataframe(
                df_cliente, columnas= ["Destino", "NumeroFactura", "FechaFactura",
                "Vencimiento", "total", "Saldo",
                "ReferenciaBancaria", "dias_credito", "dias_vencido"])
            archivo_salida_sinergia = CrearDoumento(df_a_mostrar, PATH_FILES_SINERGIA)
            ruta_salida = archivo_salida_sinergia.crear_documentos_csv(datos_cliente['nombre_archivo'])
            ARCHIVOS_CREADOS_PATHS.append(ruta_salida)
            INSERTS_ARCHIVOS_GENERALES.append(
                sinergia.crear_cardsystem_insert(
                    "Vencimientos",
                    datos_cliente['nombre_archivo'],
                    id_cliente))  
        else:
            CLIENTES_NO_ENCONTRADOS.append(cliente)

    sinergia.delete_vencimientos()
    registros_insert = sinergia.excect_cardsystem_insert(INSERTS_ARCHIVOS_GENERALES)
    sftp.ftp_send_list_files(HOST, PORT, USER, PSW, ARCHIVOS_CREADOS_PATHS)
    archivo_salida_sinergia.limpiar_archivos(PATH_FILES_SINERGIA)

    print(("-> Fin de la ejecucion del programa. <-".upper()))