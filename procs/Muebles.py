from pandasql import sqldf
from procs.Console import console
import pandas as pd
import timeit


def stats(dat, aut):
    with console.status("Cruce de Muebles...", spinner="aesthetic"):
        # -------------------------USANDO PANDASQL
        tic = timeit.default_timer()
        muebles = sqldf(
            """
                        select * from aut
                        where ID_PERSONA IN (
                        select dat.id_persona
                        from dat inner join
                        aut on aut.ID_PERSONA=dat.id_persona
                        GROUP BY dat.id_persona
                        HAVING COUNT(DOMINIO)>2)
                        ORDER BY ID_PERSONA"""
        )
        muebles.set_index("ID_PERSONA", inplace=True)  # type: ignore
    console.log(f"Cruce Muebles...[green]HECHO en {tic - timeit.default_timer()}")
    return muebles
