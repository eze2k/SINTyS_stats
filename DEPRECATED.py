import sys
from rich import print
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
import pandas as pd
import glob
import os
import shutil
import tarfile
import csv
from DictsAndLists import lista, listaTCA
from pandasql import sqldf
import timeit
import time
from rich.console import Console

# Inicializamos la consola de rich
console = Console(log_path=False)


def p30stat(paquete):
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
                txt,
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
                txt,
                sep="\t",
                encoding="ansi",
                # dtype=file["dtype"],
                index_col=0,
                engine="pyarrow",
                dtype_backend="pyarrow",
            )
            print(f"[green]{file['Nombre']}", end=" | ", flush=True)
            df.insert(loc=0, column="tabla", value=file["Nombre"])
            p30[file["Nombre"]] = df
        except IndexError:
            print(f"[red]{file['Nombre']}", end=" | ", flush=True)
            continue
    return p30
    # Cerramos el GZ para que no haya error al moverlo.
    TarP3.close()


# Tiulo
console.rule(f"[bold red]SINTyS STATS")

# Inicializamos para timear el meme
tic = timeit.default_timer()
print(f"\n")

# Leemos el p30.
start_time = time.time()
paquete30 = p30stat("Vuelta_EfectoresPaquete_30.tar.gz")
end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time:", elapsed_time, "seconds")
print(f"\n")

# Juntamos las variables para usar del dict.
# Si por alguna razon no existe el dataframe, lo rellena de uno vacio pero con
# las mismas columnas para evitar errores.
var = paquete30.get("B00", pd.DataFrame(columns=lista[0]["dtype"].keys()))
dat = paquete30.get("DATOS", pd.DataFrame(columns=lista[1]["dtype"].keys()))
bar = paquete30.get("EMBARCACIONES", pd.DataFrame(columns=lista[2]["dtype"].keys()))
avi = paquete30.get("AERONAVES", pd.DataFrame(columns=lista[3]["dtype"].keys()))
dep = paquete30.get("EMPLEO_DEPENDIENTE", pd.DataFrame(columns=lista[4]["dtype"].keys())
)
ind = paquete30.get("EMPLEO_INDEPENDIENTE", pd.DataFrame(columns=lista[5]["dtype"].keys())
)
fal = paquete30.get("FALLECIDOS", pd.DataFrame(columns=lista[6]["dtype"].keys()))
inm = paquete30.get("INMUEBLES", pd.DataFrame(columns=lista[7]["dtype"].keys()))
jub = paquete30.get("JUBILACIONES_PENSIONES", pd.DataFrame(columns=lista[8]["dtype"].keys()))
aut = paquete30.get("PADRON_AUTOMOTORES", pd.DataFrame(columns=lista[9]["dtype"].keys()))
jur = paquete30.get("PERSONAS_JURIDICAS", pd.DataFrame(columns=lista[10]["dtype"].keys()))
pnc = paquete30.get("PNC", pd.DataFrame(columns=lista[11]["dtype"].keys()))
rub = paquete30.get("RUBPS", pd.DataFrame(columns=lista[12]["dtype"].keys()))
asg = paquete30.get("ASIGNACIONES", pd.DataFrame(columns=lista[13]["dtype"].keys()))

# Preparamos el excel para el volcado final de info.
writer = pd.ExcelWriter("Evaluacion_P30.xlsx", engine="xlsxwriter")

# Preparamos el excel para el volcado final de info.
writerF = pd.ExcelWriter("Datos_P30.xlsx", engine="xlsxwriter")


# # Calculamos los stats por provincia.
# with console.status("Armando los stats por provincia...", spinner="aesthetic"):
# #-------------------------USANDO PANDASQL
#     stats_prov = sqldf(
#         """
#                     SELECT
#                     [ PROV_ACTIVIDAD  ]                      as PROVINCIA,
#                     [ LOCALIDAD  ]                           as LOCALIDAD,
#                     case when t.sexo=1 then 'M' else 'F' end as SEXO,
#                     COUNT(DISTINCT d.[id_persona ])          as EFECTORES,
#                     COUNT(DISTINCT p.id_persona)             as PNC,
#                     COUNT(DISTINCT j.id_persona)             as JUB_PEN,
#                     COUNT(DISTINCT e.id_persona)             as EMPLEO_DEP,
#                     COUNT(DISTINCT a.id_persona)             as ASIG_FAM,
#                     COUNT(DISTINCT f.id_persona)             as FALLECIDOS,
#                     COUNT(DISTINCT h.id_persona)             as JURIDICAS,
#                     COUNT(DISTINCT r.id_persona)             as RUBPS
#                     FROM      dat d
#                     LEFT JOIN pnc p on p.id_persona = d.[id_persona ]
#                     LEFT JOIN dat t on t.id_persona = d.[id_persona ]
#                     LEFT JOIN jub j on j.id_persona = d.[id_persona ]
#                     LEFT JOIN dep e on e.id_persona = d.[id_persona ]
#                     LEFT JOIN asg a on a.id_persona = d.[id_persona ]
#                     LEFT JOIN fal f on f.id_persona = d.[id_persona ]
#                     LEFT JOIN jur h on h.id_persona = d.[id_persona ]
#                     LEFT JOIN rub r on r.id_persona = d.[id_persona ]
#                     GROUP BY [ PROV_ACTIVIDAD  ],[ LOCALIDAD  ],t.sexo
#                 """
#     )

# #-------------------------USANDO PANDAS NATIVO
#     column_names = dat.columns.tolist()
#     print(column_names)
#     stats_prov = dat.merge(pnc, on='id_persona', how='left') \
#         .merge(dat, on='id_persona', how='left') \
#         .merge(jub, on='id_persona', how='left') \
#         .merge(dep, on='id_persona', how='left') \
#         .merge(asg, on='id_persona', how='left') \
#         .merge(fal, on='id_persona', how='left') \
#         .merge(jur, on='id_persona', how='left') \
#         .merge(rub, on='id_persona', how='left') \
#         .groupby(['PROV_ACTIVIDAD', 'LOCALIDAD', 'sexo']) \
#         .agg({'id_persona': 'nunique'}) \
#         .rename(columns={'id_persona': 'COUNT'}) \
#         .reset_index()

#     stats_prov['SEXO'] = stats_prov['sexo'].map({1: 'M', 2: 'F'})

#     stats_prov = stats_prov[['PROV_ACTIVIDAD', 'LOCALIDAD', 'SEXO', 'EFECTORES', 'PNC', 'JUB_PEN', 'EMPLEO_DEP', 'ASIG_FAM', 'FALLECIDOS', 'JURIDICAS', 'RUBPS']]

#     stats_prov.set_index("PROVINCIA", inplace=True)
#     stats_prov.loc["TOTAL"] = stats_prov.sum(numeric_only=True)
#     stats_prov.to_csv("Stats_por_provincia.txt", sep="\t")
# console.log("Stats por provincia...[green]HECHO")

# Cargamos los cuits de activos para cruce.
# with console.status("Cargando cuits para cruce de activos...", spinner="aesthetic"):
#     cuits = pd.read_csv("cuits.txt", sep="\t")
# console.log("Cargando cuits para cruce de activos...[green]HECHO")

# # Hacemos el cruce de activos.
# with console.status("Cruzando los cuits activos...", spinner="aesthetic"):
#     activos = sqldf(
#         """
#                     SELECT
#                     [ PROV_ACTIVIDAD  ] as PROVINCIA,
#                     COUNT(DISTINCT d.[id_persona ])                                                  as EFECTORES,
#                     SUM(case when c.cuit is not null                              then 1 else 0 end) as EFECTORES_activos,
#                     COUNT(DISTINCT p.id_persona)                                                     as PNC,
#                     SUM(case when c.cuit is not null and p.id_persona is not null then 1 else 0 end) as PNC_activos,
#                     COUNT(DISTINCT j.id_persona)                                                     as JUB_PEN,
#                     SUM(case when c.cuit is not null and j.id_persona is not null then 1 else 0 end) as JUB_PEN_activos,
#                     COUNT(DISTINCT e.id_persona)                                                     as EMPLEO_DEP,
#                     SUM(case when c.cuit is not null and e.id_persona is not null then 1 else 0 end) as EMPLEO_DEP_activos,
#                     COUNT(DISTINCT a.id_persona)                                                     as ASIG_FAM,
#                     SUM(case when c.cuit is not null and a.id_persona is not null then 1 else 0 end) as ASIG_FAM_activos,
#                     COUNT(DISTINCT f.id_persona)                                                     as FALLECIDOS,
#                     SUM(case when c.cuit is not null and f.id_persona is not null then 1 else 0 end) as FALLECIDOS_activos,
#                     COUNT(DISTINCT h.id_persona)                                                     as JURIDICAS,
#                     SUM(case when c.cuit is not null and h.id_persona is not null then 1 else 0 end) as JURIDICAS_activos,
#                     COUNT(DISTINCT r.id_persona)                                                     as RUBPS,
#                     SUM(case when c.cuit is not null and r.id_persona is not null then 1 else 0 end) as RUBPS_activos
#                     FROM        var d
#                     LEFT JOIN cuits c on c.cuit                                       = d.[ CUIT  ]
#                     LEFT JOIN (select distinct id_persona from pnc) p on p.id_persona = d.[id_persona ]
#                     LEFT JOIN (select distinct id_persona from jub) j on j.id_persona = d.[id_persona ]
#                     LEFT JOIN (select distinct id_persona from dep) e on e.id_persona = d.[id_persona ]
#                     LEFT JOIN (select distinct id_persona from asg) a on a.id_persona = d.[id_persona ]
#                     LEFT JOIN (select distinct id_persona from fal) f on f.id_persona = d.[id_persona ]
#                     LEFT JOIN (select distinct id_persona from jur) h on h.id_persona = d.[id_persona ]
#                     LEFT JOIN (select distinct id_persona from rub) r on r.id_persona = d.[id_persona ]
#                     GROUP BY [ PROV_ACTIVIDAD  ]
#                 """
#     )
#     activos.set_index("PROVINCIA", inplace=True)
#     activos.loc["TOTAL"] = activos.sum(numeric_only=True)
# console.log("Cruce de cuits...[green]HECHO")


# Armamos la base de muebles apra evaluar mas facil.
with console.status("Cruce de Muebles...", spinner="aesthetic"):
    # -------------------------USANDO PANDASQL
    # muebles = sqldf(
    #     """
    #                 select * from aut
    #                 where ID_PERSONA IN (
    #                 select dat.id_persona
    #                 from dat inner join
    #                 aut on aut.ID_PERSONA=dat.id_persona
    #                 GROUP BY dat.id_persona
    #                 HAVING COUNT(DOMINIO)>2)
    #                 ORDER BY ID_PERSONA"""
    # )
    # muebles.set_index("ID_PERSONA", inplace=True)
    # -------------------------USANDO PANDASQL
    print(aut.columns)
    print(dat.columns)

    muebles = aut[
        aut.index.isin(
            dat.merge(aut, left_index=True, right_index=True)
            .groupby(level=0)
            .filter(lambda x: len(x["DOMINIO"]) > 2)
            .index
        )
    ]

    muebles = muebles.sort_index()
    muebles = muebles.sort_values("ID_PERSONA")
console.log("Cruce de Muebles...[green]HECHO")

# Armamos la base de inmuebles apra evaluar mas facil.
with console.status("Cruce de Inmuebles...", spinner="aesthetic"):
    # -------------------------USANDO PANDASQL
    # inmuebles = sqldf(
    #     """
    #                 select * from inm
    #                 where ID_PERSONA IN (
    #                 select id_persona
    #                 from inm
    #                 GROUP BY id_persona
    #                 HAVING COUNT(ID_PERSONA)>1)
    #                 ORDER BY ID_PERSONA"""
    # )
    # inmuebles.set_index("ID_PERSONA", inplace=True)
    # -------------------------USANDO PANDAS NATIVO
    inmuebles = inm[
        inm.index.isin(inm.groupby(level=0).filter(lambda x: len(x) > 1).index)
    ]

    inmuebles = inmuebles.sort_index()
console.log("Cruce de Inmuebles...[green]HECHO")

# Evaluamos el padron.
with console.status("Evaluacion de patrimonio...", spinner="aesthetic"):
    # -------------------------USANDO PANDASQL
    # eval = sqldf(
    #     """
    #             SELECT
    #             distinct(dat.id_persona),
    #             dat.cuit,
    #             dat.ndoc,
    #             dat.deno,
    #             case when dat.sexo=1 then 'M' else 'F' end as sexo,
    #             dat.fnac,
    #             fal.FFALL,
    #             dat.grado_confiab,
    #             case when inm.id_persona is not null then 'Si' else 'No' end as Inmuebles,
    #             case when aut.id_persona is not null then 'Si' else 'No' end as Muebles,
    #             case when dep.base=105               then 'Si' else 'No' end as Servicio_Domestico,
    #             case when dep.monto > 70981          then 'Si' else 'No' end as Dependiente,
    #             case when jur.id_persona is not null then 'Si' else 'No' end as Juridicas,
    #             case when jub.monto > 70981          then 'Si' else 'No' end as Jub_pen,
    #             case when fal.id_persona is not null then 'Si' else 'No' end as Fallecido
    #             from dat
    #             left join muebles aut on aut.ID_PERSONA = dat.id_persona
    #             left join inmuebles inm on inm.ID_PERSONA = dat.id_persona
    #             left join dep on dep.ID_PERSONA = dat.id_persona
    #             left join jub on jub.ID_PERSONA = dat.id_persona
    #             left join jur on jur.ID_PERSONA = dat.id_persona
    #             left join fal on fal.ID_PERSONA = dat.id_persona
    #             WHERE
    #             Inmuebles             ='Si'
    #             or Muebles            ='Si'
    #             or Servicio_Domestico ='Si'
    #             or Dependiente        ='Si'
    #             or Juridicas          ='Si'
    #             or Jub_pen            ='Si'
    #             or Fallecido          ='Si'
    #             """
    # )
    # eval.set_index("id_persona", inplace=True)
    # -------------------------USANDO PANDAS NATIVO
    print(dep.columns)
    eval = dat[["cuit", "ndoc", "deno", "sexo", "fnac", "grado_confiab"]].copy()

    dep = dep[~dep.index.duplicated()]  # Remove duplicate labels from the index
    jub = jub[~jub.index.duplicated()]  # Remove duplicate labels from the index

    dep_reset_index = dep.reset_index()
    jub_reset_index = jub.reset_index()
    eval["Inmuebles"] = (
        pd.Series(eval.index).isin(inm.index).map({True: "Si", False: "No"})
    )
    eval["Muebles"] = (
        pd.Series(eval.index).isin(aut.index).map({True: "Si", False: "No"})
    )
    eval["Servicio_Domestico"] = (
        dep_reset_index["BASE"].eq(105).map({True: "Si", False: "No"})
    )
    eval["Dependiente"] = (dep["MONTO"] > 70981).map({True: "Si", False: "No"})
    eval["Juridicas"] = (
        pd.Series(eval.index).isin(jur.index).map({True: "Si", False: "No"})
    )
    eval["Jub_pen"] = (jub["MONTO"] > 70981).map({True: "Si", False: "No"})
    eval["Fallecido"] = (
        pd.Series(eval.index).isin(fal.index).map({True: "Si", False: "No"})
    )

    filter_conditions = (
        (eval["Inmuebles"] == "Si")
        | (eval["Muebles"] == "Si")
        | (eval["Servicio_Domestico"] == "Si")
        | (eval["Dependiente"] == "Si")
        | (eval["Juridicas"] == "Si")
        | (eval["Jub_pen"] == "Si")
        | (eval["Fallecido"] == "Si")
    )

    eval = eval[filter_conditions].sort_index()
console.log("Evaluacion de patrimonio...[green]HECHO")

# Armamos las tabs del excel y lo exportamos.
with console.status("Exportando Excel evaluaciones...", spinner="aesthetic"):
    # stats_prov.to_excel(writer, sheet_name="Stats globales")
    # activos.to_excel(writer, sheet_name="Cruce Cuits")
    eval.to_excel(writer, sheet_name="Efectores fuera del marco legal")
    muebles.to_excel(writer, sheet_name="Detalle Muebles")
    inmuebles.to_excel(writer, sheet_name="Detalle Inmuebles")
    writer.close()
console.log("Exportando Excel evaluaciones...[green]HECHO")

with console.status("Exportando Excel datos...", spinner="aesthetic"):
    var.to_excel(writerF, sheet_name="varios")
    print("1")
    dat.to_excel(writerF, sheet_name="datos")
    print("2")
    bar.to_excel(writerF, sheet_name="barcos")
    print("3")
    avi.to_excel(writerF, sheet_name="aviones")
    print("4")
    dep.to_excel(writerF, sheet_name="dependientes")
    print("5")
    ind.to_excel(writerF, sheet_name="independientes")
    print("6")
    fal.to_excel(writerF, sheet_name="fallecidos")
    print("7")
    inm.to_excel(writerF, sheet_name="inmuebles")
    print("8")
    jub.to_excel(writerF, sheet_name="jubilados")
    print("9")
    aut.to_excel(writerF, sheet_name="automotores")
    print("10")
    jur.to_excel(writerF, sheet_name="juridicos")
    print("11")
    pnc.to_excel(writerF, sheet_name="pnc")
    print("12")
    rub.to_excel(writerF, sheet_name="rub")
    print("13")
    asg.to_excel(writerF, sheet_name="asignaciones familiares")
    print("14")
    writerF.close()
console.log("Exportando Excel datos...[green]HECHO")
# Timeamos el meme
toc = timeit.default_timer()
console.log(f" Tardo:{toc - tic} segundos")
