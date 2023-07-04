from pandasql import sqldf
from procs.Console import console
import pandas as pd
import timeit


def stats(dat, pnc, var, jub, dep, asg, fal, jur, rub):
    # Calculamos los stats por provincia.
    with console.status("Armando los stats por provincia...", spinner="aesthetic"):
        tic = timeit.default_timer()
        stats_prov = sqldf(
            """
                        SELECT
                        [ PROV_ACTIVIDAD  ]                      as PROVINCIA,
                        [ LOCALIDAD  ]                           as LOCALIDAD,
                        case when t.sexo=1 then 'M' else 'F' end as SEXO,
                        COUNT(DISTINCT v.[id_persona ])          as EFECTORES,
                        COUNT(DISTINCT p.id_persona)             as PNC,
                        COUNT(DISTINCT j.id_persona)             as JUB_PEN,
                        COUNT(DISTINCT e.id_persona)             as EMPLEO_DEP,
                        COUNT(DISTINCT a.id_persona)             as ASIG_FAM,
                        COUNT(DISTINCT f.id_persona)             as FALLECIDOS,
                        COUNT(DISTINCT h.id_persona)             as JURIDICAS,
                        COUNT(DISTINCT r.id_persona)             as RUBPS
                        FROM      var v
                        LEFT JOIN pnc p on p.id_persona = v.[id_persona ]
                        LEFT JOIN dat t on t.id_persona = v.[id_persona ]
                        LEFT JOIN jub j on j.id_persona = v.[id_persona ]
                        LEFT JOIN dep e on e.id_persona = v.[id_persona ]
                        LEFT JOIN asg a on a.id_persona = v.[id_persona ]
                        LEFT JOIN fal f on f.id_persona = v.[id_persona ]
                        LEFT JOIN jur h on h.id_persona = v.[id_persona ]
                        LEFT JOIN rub r on r.id_persona = v.[id_persona ]
                        GROUP BY [ PROV_ACTIVIDAD  ],[ LOCALIDAD  ],t.sexo
                    """
        )
        stats_prov.set_index("PROVINCIA", inplace=True)  # type: ignore
        stats_prov.loc["TOTAL"] = stats_prov.sum(numeric_only=True)  # type: ignore
        stats_prov.to_csv("Stats_por_provincia.txt", sep="\t")  # type: ignore
    console.log(f"Stats por provincia...[green]HECHO en {tic - timeit.default_timer()}")
    return stats_prov


def stats2(dat, pnc, var, jub, dep, asg, fal, jur, rub):
    with console.status("Armando los stats por provincia...", spinner="aesthetic"):
        # Perform left joins on the dataframes
        merged_df = var.merge(pnc, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(dat, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(jub, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(dep, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(asg, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(fal, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(jur, how="left", left_index=True, right_index=True)
        merged_df = merged_df.merge(rub, how="left", left_index=True, right_index=True)

        # Group by PROV_ACTIVIDAD, LOCALIDAD, and sexo, and calculate the counts
        grouped_df = (
            merged_df.groupby(["PROV_ACTIVIDAD", "LOCALIDAD", "sexo"])
            .agg(
                {
                    "id_persona": "nunique",
                    "id_persona_x": "nunique",
                    "id_persona_y": "nunique",
                    "id_persona_x": "nunique",
                    "id_persona_y": "nunique",
                    "id_persona_x": "nunique",
                    "id_persona_y": "nunique",
                    "id_persona": "nunique",
                }
            )
            .reset_index()
        )

        # Rename the columns
        grouped_df.columns = [
            "PROVINCIA",
            "LOCALIDAD",
            "SEXO",
            "EFECTORES",
            "PNC",
            "JUB_PEN",
            "EMPLEO_DEP",
            "ASIG_FAM",
            "FALLECIDOS",
            "JURIDICAS",
            "RUBPS",
        ]

        # Set id_persona as the index
        grouped_df.set_index("id_persona", inplace=True)
    console.log("Stats por provincia...[green]HECHO")
    return grouped_df
