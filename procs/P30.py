from rich import print
from rich.prompt import Prompt
import pandas as pd
import tarfile
from procs.DictsAndLists import lista, listaTCA
from pandasql import sqldf
from rich.console import Console
import timeit

# Inicializamos la consola de rich
console = Console(log_path=False)


def lectura(paquete):
    tic = timeit.default_timer()
    # Abrimos el GZ en python para manosearlo por dentro.
    try:
        TarP3 = tarfile.open(paquete, "r:gz")
    except Exception as e:
        console.log("Hubo un error leyendo el archivo de vuelta.")
        Prompt.ask("\nPresione [red][ENTER][/red] para volver al menu.")
        return 0
    console.log(f"Encontrado: {paquete}")
    p30 = {}
    # Buscamos los archivos dentro del GZ.
    console.log("Leyendo vuelta SyNTIS.")
    console.log(f"Leyendo DATOS:")
    # Empezamos con el dict "lista"
    for file in lista:
        try:
            filename = [name for name in TarP3.getnames() if file["Nombre"] in name]
            txt = TarP3.extractfile(filename[0])
            df = pd.read_csv(
                txt,# type: ignore
                sep="\t",
                encoding="ansi",
                # dtype=file["dtype"],
                names=file["dtype"].keys(),
                header=0,
                index_col=0,
                engine="pyarrow",
                dtype_backend="pyarrow",
            )
            p30[file["Nombre"]] = df
            print(f"[green]{file['Nombre']}", end=" | ", flush=True)
        except IndexError:
            print(f"[red]{file['Nombre']}", end=" | ", flush=True)
            continue
    # Continuamos con el dict "listaTCA"
    print(f"\n")
    console.log(f"Leyendo CODIGOS:")
    for file in listaTCA:
        try:
            filename = [name for name in TarP3.getnames() if file["Nombre"] in name]
            txt = TarP3.extractfile(filename[0])
            df = pd.read_csv(
                txt,# type: ignore
                sep="\t",
                encoding="ansi",
                dtype=file["dtype"],
                names=file["dtype"].keys(),
                skiprows=1,
                engine="pyarrow",
                dtype_backend="pyarrow",
            )
            df.set_index(df.columns[0], inplace=True)
            print(f"[green]{file['Nombre']}", end=" | ", flush=True)
            df.insert(loc=0, column="tabla", value=file["Nombre"])
            p30[file["Nombre"]] = df
        except IndexError:
            print(f"[red]{file['Nombre']}", end=" | ", flush=True)
            continue
    print("\n")
    console.log(f"Leyendo P30...[green]HECHO en {tic - timeit.default_timer()}")
    # Cerramos el GZ para que no haya error al moverlo.
    TarP3.close()
    return p30

