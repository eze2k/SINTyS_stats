from pandasql import sqldf
from procs.Console import console
import pandas as pd
import timeit


def stats(inm):
    with console.status("Cruce de Inmuebles...", spinner="aesthetic"):
        # -------------------------USANDO PANDASQL

        tic = timeit.default_timer()
        inmuebles = sqldf(
            """
                        select * from inm
                        where ID_PERSONA IN (
                        select id_persona
                        from inm
                        GROUP BY id_persona
                        HAVING COUNT(ID_PERSONA)>1)
                        ORDER BY ID_PERSONA"""
        )
        inmuebles.set_index("ID_PERSONA", inplace=True)  # type: ignore
    console.log(f"Cruce Inmuebles...[green]HECHO en {tic - timeit.default_timer()}")
    return inmuebles
