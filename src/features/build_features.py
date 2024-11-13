import numpy as np
import pandas as pd


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


def conteo_agrupado_de_variable(
    df, vars_groupby, col_a_contar, cols_para_llave, variable_analizada
):
    """
    Perform grouped counting and aggregation of a variable in a DataFrame.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        vars_groupby (list[str]): Columns by which to group the DataFrame.
        col_a_contar (str): Column containing values to be counted.
        cols_para_llave (list[str]): Columns to be used as keys for aggregation.
        variable_analizada (str): Name of the variable being analyzed.

    Returns:
        pandas.DataFrame: A DataFrame containing grouped counts and aggregated data.
    """
    columnas_finales = vars_groupby + [col_a_contar]
    if not all(col in columnas_finales for col in cols_para_llave):
        raise ValueError(
            "Tus columnas para hacer la llave estan ausentes en las columnas finales. "
            "Debes utilizar columnas que esten en la agrupacion + variable de conteo "
            "para hacer una llave valida"
        )

    if df.empty:
        return None

    resultado = (
        df.groupby(vars_groupby, dropna=False)[col_a_contar]
        .value_counts()
        .reset_index(name=f"conteo_{variable_analizada}")
    )

    resultado["llave_id"] = resultado[cols_para_llave].astype(str).apply("-".join, axis=1)
    return resultado


def obtener_desglose_sociodemografico(
    df, vars_groupby_estatico, vars_groupby_dinamico, col_a_contar
):
    """
    Obtain demographic breakdown of a specified variable within static and dynamic groups.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        vars_groupby_estatico (list[str]): Columns for static grouping.
        vars_groupby_dinamico (list[str]): Columns for dynamic grouping.
        col_a_contar (str): Column containing values to be counted.

    Returns:
        dict: A dictionary containing breakdown results for static and dynamic groups.
    """

    # Aqui se va a tener un groupby estatico. Ej: Anios
    # Se va a tener un groupby dinamico. Ej: Sexo, Otros
    # Y se va a tener una variable que se va a contar. Ej: Diagnostico

    dict_resultados = {}
    cols_para_llave = vars_groupby_estatico + [col_a_contar]

    conteo_global = conteo_agrupado_de_variable(
        df, vars_groupby_estatico, col_a_contar, cols_para_llave, "global"
    )

    dict_resultados["global"] = conteo_global

    for variable in vars_groupby_dinamico:
        # Por lo tanto, aqui el groupby que se hara sera ["Anios", "Sexo"].
        desglose_dinamico = vars_groupby_estatico + [variable]
        conteo_dinamico = conteo_agrupado_de_variable(
            df, desglose_dinamico, col_a_contar, cols_para_llave, variable
        )
        conteo_dinamico = conteo_dinamico.drop(columns=cols_para_llave)

        dict_resultados[variable] = conteo_dinamico

    return dict_resultados


def obtener_cartera_de_procedimientos_por_diagnostico(proced_con_diagnosticos):
    conteo_procedimientos_por_diagnostico = (
        proced_con_diagnosticos.groupby(["ANIO_EGRESO", "DIAGNOSTICO1"])["procedimiento"]
        .value_counts()
        .reset_index(name="cantidad_procedimientos")
    )

    cantidad_pacientes_por_diags = (
        proced_con_diagnosticos.groupby(["ANIO_EGRESO", "DIAGNOSTICO1"])["CIP_ENCRIPTADO"]
        .nunique()
        .reset_index(name="cantidad_pacientes_distintos")
    )

    proceds_por_diagnosticos_y_pacientes = pd.merge(
        conteo_procedimientos_por_diagnostico,
        cantidad_pacientes_por_diags,
        how="inner",
        on=["ANIO_EGRESO", "DIAGNOSTICO1"],
    )

    proporcion_de_proceds = (
        proceds_por_diagnosticos_y_pacientes["cantidad_procedimientos"]
        / proceds_por_diagnosticos_y_pacientes["cantidad_pacientes_distintos"]
    )

    proceds_por_diagnosticos_y_pacientes["cantidad_proced_por_pacientes"] = proporcion_de_proceds

    return proceds_por_diagnosticos_y_pacientes


def calculate_and_add_difference(df, column_pairs):
    """
    Calculate the absolute difference between two columns in a Pandas DataFrame
    and add the difference as a new column to the original DataFrame.

    Parameters:
    df : Pandas DataFrame
        The original DataFrame.
    column_pairs : list of tuples
        A list of tuples where each tuple contains two column names.
        The absolute difference will be calculated between these columns.

    Returns:
    Pandas DataFrame
        The DataFrame with the absolute differences added as new columns.
    """
    for pair in column_pairs:
        col1, col2 = pair
        diff_col_name = f"{col1}_{col2}_difference"
        df[diff_col_name] = (df[col1] - df[col2]).abs()
    return df


def transformar_columnas_servicio(df, columnas_servicio, variables_id):
    """Transforma las columnas de servicio a formato long."""
    return pd.melt(
        df,
        id_vars=variables_id,
        value_vars=columnas_servicio,
        var_name="tipo_servicio",
        value_name="servicio",
    )


def transformar_columnas_fecha(df, columnas_fecha, variables_id):
    """Transforma las columnas de fechas a formato long."""
    return pd.melt(
        df,
        id_vars=variables_id,
        value_vars=columnas_fecha,
        var_name="tipo_fecha",
        value_name="fecha",
    )


def crear_viaje_paciente(df, columnas_servicio, columnas_fecha):
    """Crea el viaje del paciente uniendo las columnas de servicio y de fechas en formato long."""
    variables_id_servicio = [
        "id_egreso",
        "DIAGNOSTICO1",
        "ANIO_EGRESO",
        "IR_29301_SEVERIDAD",
        "CIP_ENCRIPTADO",
    ]
    variables_id_fecha = ["id_egreso"]

    servicio_long = transformar_columnas_servicio(df, columnas_servicio, variables_id_servicio)
    fecha_long = transformar_columnas_fecha(df, columnas_fecha, variables_id_fecha)

    return fecha_long.join(servicio_long, rsuffix="_y")


def recodificar_tipo_servicio(df, categorias):
    """Recodifica los tipos de servicio para ordenarlos correctamente."""
    df["tipo_servicio"] = pd.Categorical(df["tipo_servicio"], categories=categorias, ordered=True)
    return df


def recodificar_tipo_cama(df, categoria_camas):
    df["servicio"] = df["servicio"].replace(categoria_camas)
    return df


def ordenar_por_linea_temporal(df, columnas_orden):
    """Ordena el DataFrame por las columnas especificadas para seguir la línea temporal."""
    return df.sort_values(columnas_orden)


def eliminar_traslados_sin_ingresar(df):
    """Elimina los registros que tienen valores NaN."""
    return df.dropna()


def reiniciar_indice(df):
    """Reinicia el índice del DataFrame."""
    return df.reset_index(drop=True)


def formatear_fechas(df):
    """Formatea las fechas ingresadas"""
    # Formatea las fechas con el dia al inicio (egresos del 2023) y con el anio al inicio
    # (egresos entre 2019 a 2022)
    fechas_dia_inicial = pd.to_datetime(df["fecha"], format="%d-%m-%Y", errors="coerce")
    fechas_anio_inicial = pd.to_datetime(df["fecha"], format="%Y-%m-%d", errors="coerce")

    # Une ambas fechas en una columna
    fechas_consolidadas = np.where(
        fechas_dia_inicial.isna(), fechas_anio_inicial, fechas_dia_inicial
    )

    df["fecha"] = fechas_consolidadas
    return df


def procesar_viaje_paciente(viaje_paciente, categorias, columnas_orden, codificador_camas):
    """Función principal para procesar el viaje del paciente en varios pasos."""
    viaje_paciente = viaje_paciente.copy()

    df = recodificar_tipo_servicio(viaje_paciente, categorias)
    df = recodificar_tipo_cama(df, codificador_camas)
    df = ordenar_por_linea_temporal(df, columnas_orden)
    df = eliminar_traslados_sin_ingresar(df)
    df = reiniciar_indice(df)
    df = formatear_fechas(df)

    return df


def calcular_estadia(df):
    """Calcula la duración de la estadía de cada traslado, en base a la diferencia entre fechas."""
    df["duracion_estadia"] = df.groupby("id_egreso")["fecha"].diff().shift(-1)
    return df


def corregir_duracion_negativa(df):
    """Corrige las duraciones negativas añadiendo un año a la fecha correspondiente.

    Estas fechas tienen duraciones negativas, ya que la fecha inmediatamente posterior tiene el
    anio mal imputado. Por lo tanto, se agrega un anio a tal fecha
    """
    indices_negativos = df[df["duracion_estadia"] < pd.Timedelta(0)].index + 1
    df.loc[indices_negativos, "fecha"] += pd.offsets.DateOffset(years=1)
    df["duracion_estadia"] = df.groupby("id_egreso")["fecha"].diff().shift(-1)
    return df


def corregir_duracion_cero(df):
    """Corrige las duraciones de estadía igual a cero añadiendo un día."""
    indices_cero = df[df["duracion_estadia"] == pd.Timedelta(0)].index
    df.loc[indices_cero, "duracion_estadia"] += pd.Timedelta(days=1)
    return df


def procesar_duracion_estadia(df):
    """Función principal que calcula la estadía y corrige duraciones negativas o iguales a cero."""
    df = calcular_estadia(df)
    df = corregir_duracion_negativa(df)
    df = corregir_duracion_cero(df)
    return df
