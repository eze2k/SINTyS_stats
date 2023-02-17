import pandas as pd
import glob
import os
import shutil
import tarfile
from sqlalchemy import create_engine, exc
import csv
from procesos.DictsAndLists import lista, listaTCA
from procesos.chunkerPD import subir_con_progreso, chunker, si_o_no, decrypt_file
import gnupg
from rich import print
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
from procesos import discordbot as dbot


def go(engine, f):
    """
    [PROCESA EL P3 Y GUARDA LOS ARCHIVOS EN SUS CARPETAS]

    Arguments:
        conn {[sqlalchemy]} -- [conneccion de sqlalchemy]
        engine {[sqlalchemy]} -- [engine creado por sqlalchemy]
        f {[figlet]} -- [toma el tipo de letra para marquee]

    Returns:
        [0] -- [Salida al menu]
    """

    print(
        Panel(
            Text("Carga la vuelta SINTyS y evalua.", justify="center"),
            title="[red]PAQUETE 3",
        )
    )
    conn = engine.connect()

    # Buscamos archivos que terminen en PGP.
    print("Buscando paquete...")
    crypto = glob.glob("*.pgp")

    # Si encuentra 1 sigue,mas de uno temrina.
    if crypto and len(crypto) == 1:
        # Armamos la salida del decrypt sacandole los ultimos 4 lugares a la direccion.
        paquete = crypto[0][: len(crypto[0]) - 4]

        # Desencriptamos.
        decrypt_file(crypto[0], "Eze2kftw!", paquete)

        # Abrimos el GZ en python para manosearlo por dentro.
        try:
            TarP3 = tarfile.open(paquete, "r:gz")
        except Exception as e:
            print("Hubo un error leyendo el archivo de vuelta.")
            dbot.log_discord(
                "Paquete 03 ERROR",
                {"Descripcion": f"Al leer el archivo de vuelta:\n{str(e)}"},
            )
            Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
            return 0
        print(f"Encontrado: {paquete}")
    else:
        # Si no hay nada, termina el proceso, lo mismo si encuentra mas de un paquete.
        print(
            """
            No se proceso por una de dos razones:\n
            1- No habia archivo para procesar.\n
            2- Habia mas de un archivo para procesar.\n
        """
        )
        Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
        return 0
    print("Listo.")

    if si_o_no("Subir archivos?"):
        # Buscamos los archivos dentro del GZ y los subimos a la DB.
        print("Subiendo vuelta SyNTIS.")
        for file in lista:
            conn.execute(f"DELETE FROM DTS_EntradaSintys_{file['Nombre']}_2019")
            try:
                filename = [name for name in TarP3.getnames() if file["Nombre"] in name]

                # En el caso de B00 hay chances de que venga un archivo VARIOS.B00, asi q lo filtramos.
                if file["Nombre"] == "B00":
                    filename = [k for k in filename if not "VARIOS" in k]

                txt = TarP3.extractfile(filename[0])
                df = pd.read_csv(
                    txt,
                    sep="\t",
                    encoding="ansi",
                    dtype=file["dtype"],
                    names=file["dtype"].keys(),
                    header=0,
                )
                print(f"Subiendo {file['Nombre']}")
                subir_con_progreso(
                    df, engine, f"DTS_EntradaSintys_{file['Nombre']}_2019", repl=False
                )
                # df.to_sql(f"DTS_EntradaSintys_{file['Nombre']}_2019",
                #           con=engine,
                #           if_exists="append",
                #           index=False,
                #           schema="dbo")
            except exc.SQLAlchemyError as e:
                print(f"Hubo un error procesando {file['Nombre']}:\n{str(e)}")
                conn.close()
                TarP3.close()
                Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
                return 0
            except IndexError:
                print(f"'\n{file['Nombre']} no esta en la vuelta.")
                continue

        conn.execute(f"DELETE FROM DTS_EntradaSintys_TCA_2019")

        for file in listaTCA:
            try:
                filename = [name for name in TarP3.getnames() if file["Nombre"] in name]
                txt = TarP3.extractfile(filename[0])
                df = pd.read_csv(txt, sep="\t", encoding="ansi", dtype=file["dtype"])
                print(f"Subiendo {file['Nombre']}")

                df.insert(loc=0, column="tabla", value=file["Nombre"])
                subir_con_progreso(df, engine, "DTS_EntradaSintys_TCA_2019", repl=False)
                # df.to_sql(f"DTS_EntradaSintys_TCA_2019",
                #           con=engine,
                #           if_exists="append",
                #           index=False,
                #           schema="dbo")
            except exc.SQLAlchemyError as e:
                print(f"Hubo un error procesando {file['Nombre']}:\n{str(e)}")
                conn.close()
                TarP3.close()
                Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
                return 0
            except IndexError:
                print(f"\n{file['Nombre']} no esta en la vuelta.\n")
                continue

    else:
        pass

    # Cerramos el GZ para que no haya error al moverlo.
    TarP3.close()

    if si_o_no("Ejecutar store ENTRADA P3?"):
        # Ejecutamos el proceso de p3 de entrada con SQLACHEMY.
        print("Ejecutando store ENTRADA P3.")
        try:
            with engine.connect() as conn, conn.begin():
                conn.execute("EXEC proc_P3_02_2019")
        except Exception as e:
            print(f"Hubo un error procesando el paquete:\n{str(e)}")
            dbot.log_discord(
                "Paquete 03 ERROR",
                {"Descripcion": f"Al ejecutar store de ENTRADA:\n{str(e)}"},
            )
            Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
            return 0
        print("Entrada del P3[green]completada.[/green]")
        num = pd.read_sql_query(
            "SELECT [numero]  FROM [adm_efectores].[dbo].[Numeracion_Paquetes]  where paquete='p3-2019'",
            con=engine,
        )
        nlote = str(num.iloc[0]["numero"])
        dbot.log_discord(
            "Paquete 03", {f"Lote {nlote}": "ENTRADA ejecutada con exito."}
        )
    else:
        pass

    if si_o_no("Ejecutar store EVALUACION?"):
        # Ejecutamos el proceso de p3 con SQLACHEMY.
        print("Ejecutando store EVALUACION.")
        try:
            with engine.connect() as conn, conn.begin():
                conn.execute("EXEC proc_P3_03_2019")
        except Exception as e:
            print(f"Hubo un error procesando el paquete:\n{str(e)}")
            dbot.log_discord(
                "Store EVALUACION ERROR",
                {"Descripcion": f"Al ejecutar el store de evaluacion:\n{str(e)}"},
            )
            Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
            return 0
        print("Evaluacion completada con [green]exito[/green].")
        dbot.log_discord(
            "Store EVALUACION", {"Evaluacion automatica": "Ejecutada con exito."}
        )
    else:
        pass

    if si_o_no("Archivar y generar salida de errores?"):
        # Buscamos el numero de paquete 3 para crear la carpeta.
        num = pd.read_sql_query(
            "SELECT [numero]  FROM [adm_efectores].[dbo].[Numeracion_Paquetes]  where paquete='p3-2019'",
            con=engine,
        )
        nlote = str(num.iloc[0]["numero"])

        # Creamos el path.
        lote = "P:\\sintys\\Paquete_03\\Lote_" + nlote

        # Creamos las carpetas, si ya existian avisa y sigue.
        try:
            os.mkdir(f"{lote}")
            print(f"Se creo la carpeta {lote}.")
        except:
            print("La carpeta del P3 este lote ya existia.")

        # Movemos los archivos de vuelta de SiNTYS.
        gz = glob.glob("*.gz")
        pgp = glob.glob("*.pgp")
        try:
            shutil.move(gz[0], f"{lote}\\{nlote}.tar.gz")
        except:
            print("tar no encontrado")
            pass
        try:
            shutil.move(pgp[0], f"{lote}\\{nlote}.pgp")
        except:
            print("pgp no encontrado")
            pass
        print("Todos los archivos fueron archivados(lol).")
        # Creamos la salida con los errores SINTYS.
        try:
            errores1 = pd.read_sql_table(
                "DTS_ErroresSintys_B00_2019", con=engine, schema="dbo"
            )
            errores1.to_excel(f"{lote}\\{nlote}-errores-SINTYS.xlsx")
            print(f"Generado errores SINTYS : {len(errores1)}")
        except:
            print("Hubo un problema generando el archivo de error.")
        # Creamos la salida con los errores.
        try:
            errores2 = pd.read_sql_table(
                "DTS_ErroresPaquete03_2019", con=engine, schema="dbo"
            )
            errores2.to_excel(
                f"{lote}\\{nlote}-errores.xlsx",
            )
            print(f"Generado errores varios : {len(errores2)}")
        except:
            print("Hubo un problema generando el archivo de error.")

        # Creamos la salida con los caidos.
        try:
            caidos = pd.read_sql_table("Listado_Caidos", con=engine, schema="dbo")
            caidos.to_excel(f"{lote}\\{nlote}-caidos.xlsx")
            print(f"Generado caidos 26 para edicion: {len(caidos)}")
        except:
            print("Hubo un problema generando el archivo de caidos.")

    else:
        pass
    dbot.log_discord(
        "Reporte P3",
        {
            "Caidos 26(CABA)": len(caidos),
            "Errores -1": len(errores1),
            "Errores P3": len(errores2),
        },
    )
    # Terminamos.
    conn.close()
    Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
    return 0
