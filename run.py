from procs import P30, Prov, Muebles, Inmuebles, Evaluar
from rich import print
from rich.prompt import Prompt
from rich.console import Console
from procs.DictsAndLists import lista
import pandas as pd
import timeit

# Timear el proceso
ticg = timeit.default_timer()

# Inicializamos la consola de rich
console = Console(log_path=False)

# Tiulo
console.rule(f"[bold red]SINTyS STATS")

# Leemos el p30.
paquete30 = P30.lectura("Vuelta_EfectoresPaquete_30.tar.gz")

# Juntamos las variables para usar del dict.
# Si por alguna razon no existe el dataframe, lo rellena de uno vacio pero con
# las mismas columnas para evitar errores.
# fmt: off
var = paquete30.get("B00", pd.DataFrame(columns=lista[0]["dtype"].keys())) # type: ignore
dat = paquete30.get("DATOS", pd.DataFrame(columns=lista[1]["dtype"].keys())) # type: ignore
bar = paquete30.get("EMBARCACIONES", pd.DataFrame(columns=lista[2]["dtype"].keys()))# type: ignore
avi = paquete30.get("AERONAVES", pd.DataFrame(columns=lista[3]["dtype"].keys())) # type: ignore # type: ignore
dep = paquete30.get("EMPLEO_DEPENDIENTE", pd.DataFrame(columns=lista[4]["dtype"].keys())) # type: ignore
ind = paquete30.get("EMPLEO_INDEPENDIENTE", pd.DataFrame(columns=lista[5]["dtype"].keys()))# type: ignore
fal = paquete30.get("FALLECIDOS", pd.DataFrame(columns=lista[6]["dtype"].keys())) # type: ignore
inm = paquete30.get("INMUEBLES", pd.DataFrame(columns=lista[7]["dtype"].keys()))# type: ignore
jub = paquete30.get("JUBILACIONES_PENSIONES", pd.DataFrame(columns=lista[8]["dtype"].keys())) # type: ignore
aut = paquete30.get("PADRON_AUTOMOTORES", pd.DataFrame(columns=lista[9]["dtype"].keys())) # type: ignore
jur = paquete30.get("PERSONAS_JURIDICAS", pd.DataFrame(columns=lista[10]["dtype"].keys()))# type: ignore
pnc = paquete30.get("PNC", pd.DataFrame(columns=lista[11]["dtype"].keys())) # type: ignore
rub = paquete30.get("RUBPS", pd.DataFrame(columns=lista[12]["dtype"].keys()))# type: ignore
asg = paquete30.get("ASIGNACIONES", pd.DataFrame(columns=lista[13]["dtype"].keys()))# type: ignore
# fmt: on

# Preparamos el excel para el volcado final de info.
writer = pd.ExcelWriter("Evaluacion_P30.xlsx", engine="xlsxwriter")

# Preparamos el excel para el volcado final de info.
writerF = pd.ExcelWriter("Datos_P30.xlsx", engine="xlsxwriter")

# Hacemos los diferentes cruces que requerimos.
muebles_stats = Muebles.stats(dat, aut)
inmuebles_stats = Inmuebles.stats(inm)
eval_stats = Evaluar.stats(dat, jub, dep, jur, fal, muebles_stats, inmuebles_stats)
prov_stats = Prov.stats(dat, pnc, var, jub, dep, asg, fal, jur, rub)

# Armamos las tabs del excel y lo exportamos.
with console.status("Exportando Excel evaluaciones...", spinner="aesthetic"):
    tic = timeit.default_timer()
    # stats_prov.to_excel(writer, sheet_name="Stats globales")
    eval_stats.to_excel(writer, sheet_name="Efectores fuera del marco legal")  # type: ignore
    muebles_stats.to_excel(writer, sheet_name="Detalle Muebles")  # type: ignore
    inmuebles_stats.to_excel(writer, sheet_name="Detalle Inmuebles")  # type: ignore
    writer.close()
console.log(
    f"Exportando Excel evaluaciones...[green]HECHO en {tic - timeit.default_timer()}"
)

# with console.status("Exportando Excel datos...", spinner="aesthetic"):
#     tic = timeit.default_timer()
#     var.to_excel(writerF, sheet_name="varios")
#     dat.to_excel(writerF, sheet_name="datos")
#     bar.to_excel(writerF, sheet_name="barcos")
#     avi.to_excel(writerF, sheet_name="aviones")
#     dep.to_excel(writerF, sheet_name="dependientes")
#     ind.to_excel(writerF, sheet_name="independientes")
#     fal.to_excel(writerF, sheet_name="fallecidos")
#     inm.to_excel(writerF, sheet_name="inmuebles")
#     jub.to_excel(writerF, sheet_name="jubilados")
#     aut.to_excel(writerF, sheet_name="automotores")
#     jur.to_excel(writerF, sheet_name="juridicos")
#     pnc.to_excel(writerF, sheet_name="pnc")
#     rub.to_excel(writerF, sheet_name="rub")
#     asg.to_excel(writerF, sheet_name="asignaciones familiares")
#     writerF.close()
# console.log(f"Exportando Excel datos...[green]HECHO en {tic - timeit.default_timer()}")

console.log(f"PROCESO...[green]COMPLETADO en {ticg - timeit.default_timer()}")
