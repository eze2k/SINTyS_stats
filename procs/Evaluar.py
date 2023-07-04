from pandasql import sqldf
from procs.Console import console
import pandas as pd
import timeit


def stats(dat, jub, dep, jur, fal, muebles, inmuebles):
    with console.status("Evaluando...", spinner="aesthetic"):
        # -------------------------USANDO PANDASQL
        tic = timeit.default_timer()
        eval = sqldf(
            """
                    SELECT
                    distinct(dat.id_persona),
                    dat.cuit,
                    dat.ndoc,
                    dat.deno,
                    case when dat.sexo=1 then 'M' else 'F' end as sexo,
                    dat.fnac,
                    fal.FFALL,
                    dat.grado_confiab,
                    case when inm.id_persona is not null then 'Si' else 'No' end as Inmuebles,
                    case when aut.id_persona is not null then 'Si' else 'No' end as Muebles,
                    case when dep.base=105               then 'Si' else 'No' end as Servicio_Domestico,
                    case when dep.monto > 70981          then 'Si' else 'No' end as Dependiente,
                    case when jur.id_persona is not null then 'Si' else 'No' end as Juridicas,
                    case when jub.monto > 70981          then 'Si' else 'No' end as Jub_pen,
                    case when fal.id_persona is not null then 'Si' else 'No' end as Fallecido
                    from dat
                    left join muebles aut on aut.ID_PERSONA = dat.id_persona
                    left join inmuebles inm on inm.ID_PERSONA = dat.id_persona
                    left join dep on dep.ID_PERSONA = dat.id_persona
                    left join jub on jub.ID_PERSONA = dat.id_persona
                    left join jur on jur.ID_PERSONA = dat.id_persona
                    left join fal on fal.ID_PERSONA = dat.id_persona
                    WHERE
                    Inmuebles             ='Si'
                    or Muebles            ='Si'
                    or Servicio_Domestico ='Si'
                    or Dependiente        ='Si'
                    or Juridicas          ='Si'
                    or Jub_pen            ='Si'
                    or Fallecido          ='Si'
                    """
        )
        eval.set_index("id_persona", inplace=True)  # type: ignore
        console.log(f"Evaluando...[green]HECHO en {tic - timeit.default_timer()}")
    return eval
