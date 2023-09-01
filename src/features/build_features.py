import numpy as np


def contar_procedimientos_y_rankear(df_procedimientos_long):
    conteo_procedimientos = (
        df_procedimientos_long.groupby(["ANIO_EGRESO", "COD_HOSPITAL"])["procedimiento"]
        .value_counts()
        .reset_index(name="conteo")
        .sort_values(["ANIO_EGRESO", "procedimiento", "conteo"], ascending=False)
    )

    ranking_por_procedimiento = (
        conteo_procedimientos.groupby(["ANIO_EGRESO", "procedimiento"]).cumcount() + 1
    )

    diferencia_entre_hospitales_contiguos = conteo_procedimientos["conteo"].diff(periods=-1)

    total_procedimientos = conteo_procedimientos.groupby(["ANIO_EGRESO", "procedimiento"])[
        "conteo"
    ].transform("sum")

    conteo_procedimientos["ranking"] = ranking_por_procedimiento
    conteo_procedimientos["diferencia_proximo_hospital"] = diferencia_entre_hospitales_contiguos
    conteo_procedimientos["total_procedimientos"] = total_procedimientos

    return conteo_procedimientos
