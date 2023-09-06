import numpy as np


def contar_con_agrupacion(df, agrupacion, var_categorica_a_contar, nombre_var_conteo):
    conteo_agrupado = (
        df.groupby(agrupacion)[var_categorica_a_contar]
        .value_counts()
        .reset_index(name=nombre_var_conteo)
    )
    return conteo_agrupado


def agregar_ranking_sobre_un_grupo(df_conteo, grupo_ranking, var_para_ranking):
    tmp = df_conteo.sort_values(grupo_ranking + [var_para_ranking], ascending=False)
    tmp[f"ranking_{var_para_ranking}"] = tmp.groupby(grupo_ranking).cumcount() + 1

    return tmp


def obtener_totales_sobre_un_grupo(df, grupo_a_sumar, var_a_sumar):
    return df.groupby(grupo_a_sumar)[var_a_sumar].transform("sum")


def obtener_diferencias_entre_filas_de_serie(serie):
    return serie.diff(periods=-1).astype("Int32")


def calcular_conteo_y_ranking_sobre_variable_categorica(
    df, var_categ_a_contar, agrupacion, grupo_ranking
):
    var_conteo = f"conteo_{var_categ_a_contar}"
    var_ranking = f"ranking_{var_categ_a_contar}"
    var_diferencias = f"diferencias_{var_categ_a_contar}"
    var_totales = f"totales_{var_categ_a_contar}"
    var_proporcion = f"proporcion_{var_categ_a_contar}"

    conteo = contar_con_agrupacion(df, agrupacion, var_categ_a_contar, var_conteo)
    conteo = agregar_ranking_sobre_un_grupo(conteo, grupo_ranking, var_conteo)
    conteo[var_diferencias] = obtener_diferencias_entre_filas_de_serie(conteo[var_conteo])
    conteo[var_totales] = obtener_totales_sobre_un_grupo(conteo, grupo_ranking, var_conteo)
    conteo[var_proporcion] = conteo[var_conteo] / conteo[var_totales]

    return conteo
