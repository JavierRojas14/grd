def contar_procedimientos_y_rankear(df_procedimientos_long):
    conteo_procedimientos = (
        df_procedimientos_long.groupby(["ANIO_EGRESO", "COD_HOSPITAL"])["value"]
        .value_counts()
        .reset_index(name="conteo")
        .sort_values(["ANIO_EGRESO", "value", "conteo"], ascending=False)
    )

    conteo_procedimientos["ranking"] = (
        conteo_procedimientos.groupby(["ANIO_EGRESO", "value"]).cumcount() + 1
    )

    conteo_procedimientos["diferencia_proximo_hospital"] = conteo_procedimientos["conteo"].diff(
        periods=-1
    )

    conteo_procedimientos["diferencia_proximo_hospital"] = np.where(
        conteo_procedimientos["diferencia_proximo_hospital"] < 0,
        -1,
        conteo_procedimientos["diferencia_proximo_hospital"],
    )

    return conteo_procedimientos
