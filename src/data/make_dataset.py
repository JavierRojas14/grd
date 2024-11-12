# -*- coding: utf-8 -*-
import glob
import logging
from pathlib import Path

import click
import numpy as np
import pandas as pd
import polars as pl
from dotenv import find_dotenv, load_dotenv

COLUMNAS_POLARS = {
    "COD_HOSPITAL": pl.Int32,
    "CIP_ENCRIPTADO": pl.Int32,
    "SEXO": pl.Categorical,
    "FECHA_NACIMIENTO": pl.Date,
    "ETNIA": pl.Categorical,
    "PROVINCIA": pl.Categorical,
    "COMUNA": str,
    "NACIONALIDAD": pl.Categorical,
    "PREVISION": pl.Categorical,
    "SERVICIO_SALUD": pl.Categorical,
    "TIPO_PROCEDENCIA": pl.Categorical,
    "TIPO_INGRESO": pl.Categorical,
    "ESPECIALIDAD_MEDICA": pl.Categorical,
    "TIPO_ACTIVIDAD": pl.Categorical,
    "FECHA_INGRESO": pl.Date,
    "SERVICIOINGRESO": pl.Categorical,
    "FECHATRASLADO1": pl.Date,
    "SERVICIOTRASLADO1": pl.Categorical,
    "FECHATRASLADO2": pl.Date,
    "SERVICIOTRASLADO2": pl.Categorical,
    "FECHATRASLADO3": pl.Date,
    "SERVICIOTRASLADO3": pl.Categorical,
    "FECHATRASLADO4": pl.Date,
    "SERVICIOTRASLADO4": pl.Categorical,
    "FECHATRASLADO5": pl.Date,
    "SERVICIOTRASLADO5": pl.Categorical,
    "FECHATRASLADO6": pl.Date,
    "SERVICIOTRASLADO6": pl.Categorical,
    "FECHATRASLADO7": pl.Date,
    "SERVICIOTRASLADO7": pl.Categorical,
    "FECHATRASLADO8": pl.Date,
    "SERVICIOTRASLADO8": pl.Categorical,
    "FECHATRASLADO9": pl.Date,
    "SERVICIOTRASLADO9": pl.Categorical,
    "FECHAALTA": pl.Date,
    "SERVICIOALTA": pl.Categorical,
    "TIPOALTA": pl.Categorical,
    "CONDICIONDEALTANEONATO1": pl.Categorical,
    "PESORN1": pl.Int16,
    "SEXORN1": pl.Categorical,
    "RN1ESTADO": pl.Int8,
    "CONDICIONDEALTANEONATO2": pl.Categorical,
    "PESORN2": pl.Int16,
    "SEXORN2": pl.Categorical,
    "RN2ESTADO": pl.Int8,
    "CONDICIONDEALTANEONATO3": pl.Categorical,
    "PESORN3": pl.Int16,
    "SEXORN3": pl.Categorical,
    "RN3ESTADO": pl.Int8,
    "CONDICIONDEALTANEONATO4": pl.Categorical,
    "PESORN4": pl.Int16,
    "SEXORN4": pl.Categorical,
    "RN4ESTADO": pl.Int8,
    "DIAGNOSTICO1": str,
    "DIAGNOSTICO2": str,
    "DIAGNOSTICO3": str,
    "DIAGNOSTICO4": str,
    "DIAGNOSTICO5": str,
    "DIAGNOSTICO6": str,
    "DIAGNOSTICO7": str,
    "DIAGNOSTICO8": str,
    "DIAGNOSTICO9": str,
    "DIAGNOSTICO10": str,
    "DIAGNOSTICO11": str,
    "DIAGNOSTICO12": str,
    "DIAGNOSTICO13": str,
    "DIAGNOSTICO14": str,
    "DIAGNOSTICO15": str,
    "DIAGNOSTICO16": str,
    "DIAGNOSTICO17": str,
    "DIAGNOSTICO18": str,
    "DIAGNOSTICO19": str,
    "DIAGNOSTICO20": str,
    "DIAGNOSTICO21": str,
    "DIAGNOSTICO22": str,
    "DIAGNOSTICO23": str,
    "DIAGNOSTICO24": str,
    "DIAGNOSTICO25": str,
    "DIAGNOSTICO26": str,
    "DIAGNOSTICO27": str,
    "DIAGNOSTICO28": str,
    "DIAGNOSTICO29": str,
    "DIAGNOSTICO30": str,
    "DIAGNOSTICO31": str,
    "DIAGNOSTICO32": str,
    "DIAGNOSTICO33": str,
    "DIAGNOSTICO34": str,
    "DIAGNOSTICO35": str,
    "PROCEDIMIENTO1": str,
    "PROCEDIMIENTO2": str,
    "PROCEDIMIENTO3": str,
    "PROCEDIMIENTO4": str,
    "PROCEDIMIENTO5": str,
    "PROCEDIMIENTO6": str,
    "PROCEDIMIENTO7": str,
    "PROCEDIMIENTO8": str,
    "PROCEDIMIENTO9": str,
    "PROCEDIMIENTO10": str,
    "PROCEDIMIENTO11": str,
    "PROCEDIMIENTO12": str,
    "PROCEDIMIENTO13": str,
    "PROCEDIMIENTO14": str,
    "PROCEDIMIENTO15": str,
    "PROCEDIMIENTO16": str,
    "PROCEDIMIENTO17": str,
    "PROCEDIMIENTO18": str,
    "PROCEDIMIENTO19": str,
    "PROCEDIMIENTO20": str,
    "PROCEDIMIENTO21": str,
    "PROCEDIMIENTO22": str,
    "PROCEDIMIENTO23": str,
    "PROCEDIMIENTO24": str,
    "PROCEDIMIENTO25": str,
    "PROCEDIMIENTO26": str,
    "PROCEDIMIENTO27": str,
    "PROCEDIMIENTO28": str,
    "PROCEDIMIENTO29": str,
    "PROCEDIMIENTO30": str,
    "MEDICOINTERV1_ENCRIPTADO": pl.Int32,
    "FECHAPROCEDIMIENTO1": pl.Date,
    "FECHAINTERV1": pl.Date,
    "ESPECIALIDADINTERVENCION": pl.Categorical,
    "MEDICOALTA_ENCRIPTADO": pl.Int32,
    "USOSPABELLON": str,
    "IR_29301_COD_GRD": pl.Int32,
    "IR_29301_PESO": str,
    "IR_29301_SEVERIDAD": pl.Int8,
    "IR_29301_MORTALIDAD": pl.Int8,
    "HOSPPROCEDENCIA": str,
}

VALORES_NULOS_COLUMNAS = {
    "CIP_ENCRIPTADO": "SIN INFORMACIÓN",
    "PROCEDIMIENTO1": "DESCONOCIDO",
    "PROCEDIMIENTO2": "DESCONOCIDO",
    "PROCEDIMIENTO3": "DESCONOCIDO",
    "PROCEDIMIENTO5": "DESCONOCIDO",
    "FECHAPROCEDIMIENTO1": "14048380-K",
    "IR_29301_COD_GRD": "DESCONOCIDO",
    "IR_29301_PESO": "DESCONOCIDO",
    "IR_29301_SEVERIDAD": "DESCONOCIDO",
    "IR_29301_MORTALIDAD": "DESCONOCIDO",
    "FECHA_NACIMIENTO": "--01",
}

REEMPLAZO_PREVISION = {
    "FONASA INSTITUCIONAL - (MAI) A": "FONASA A",
    "FONASA INSTITUCIONAL - (MAI) B": "FONASA B",
    "FONASA INSTITUCIONAL - (MAI) C": "FONASA C",
    "FONASA INSTITUCIONAL - (MAI) D": "FONASA D",
}


def agregar_informacion_comuna(df):
    tmp = df.clone()

    comunas = pd.read_excel("data/external/Esquema_Registro-2023.xls", sheet_name=2, header=6)
    comunas = comunas[["Código Comuna", "Nombre Comuna", "Código Región", "Nombre Región"]]
    comunas = comunas.dropna(how="all")
    comunas["Código Comuna"] = comunas["Código Comuna"].astype(int)
    comunas["Código Región"] = comunas["Código Región"].astype(int)

    comunas = comunas.rename(columns={"Código Comuna": "cod_comuna", "Código Región": "codregion"})

    comunas = pl.from_pandas(comunas)

    tmp = tmp.join(comunas, how="inner", left_on="COMUNA", right_on="Nombre Comuna")
    return tmp


def leer_grd_no_utf8(input_folder):
    ruta_archivo = f"{input_folder}/no-utf-8/*.txt"
    archivos = glob.glob(ruta_archivo)

    df = pd.concat(
        pd.read_csv(file, sep="|", encoding="utf-16-le", on_bad_lines="skip") for file in archivos
    )

    return df


def leer_grd_con_una_columna_mas(input_folder):
    ruta_archivo = f"{input_folder}/grd_con_una_columna_mas/GRD_PUBLICO_EXTERNO_2022.txt"

    # Lee el archivo GRD con una columna mas
    df = pd.read_csv(
        ruta_archivo,
        sep="|",
        on_bad_lines="skip",
        encoding="utf-16-le",
    )

    return df


def leer_grd(input_folder):
    """
    Reads and processes the GRD data.

    This function reads the GRD database from FONASA's hospital discharges of Chile,
    applies data transformations and returns the processed DataFrame.

    :return: The processed DataFrame containing the GRD data.
    :rtype: polars.DataFrame
    """
    with pl.StringCache():
        df = pl.scan_csv(
            f"{input_folder}/*.txt",
            separator="|",
            infer_schema_length=0,
            # dtypes=COLUMNAS_POLARS,
            # null_values=VALORES_NULOS_COLUMNAS,
        )

        df = df.with_columns(pl.col("FECHAALTA").cast(pl.Date, strict=False))
        # # Convierte las fechas a formato de fechas correctamente
        # df = df.with_columns(
        #     pl.col("FECHAALTA")
        #     .str.strptime(pl.Datetime, "%Y-%m-%d", strict=False)
        #     .alias("FECHAALTA"),
        #     pl.col("FECHAALTA")
        #     .str.strptime(pl.Datetime, "%d-%m-%Y", strict=False)
        #     .alias("FECHAALTA_NEW"),
        # )

        # df = df.with_columns(
        #     pl.when(pl.col("FECHAALTA").is_null())
        #     .then(pl.col("FECHAALTA_NEW"))
        #     .otherwise(pl.col("FECHAALTA"))
        #     .alias("FECHAALTA")
        # )

        df = df.with_columns(
            pl.col("FECHA_INGRESO").cast(pl.Date, strict=False),  # Elimina los valores --4
            pl.col("FECHA_NACIMIENTO").cast(
                pl.Date, strict=False
            ),  # Elimina los valores 'NO APLICA'
        )

        # Convierte el peso a Float
        peso_en_float = (
            pl.col("IR_29301_PESO")
            .str.replace(",", ".")
            .str.replace("DESCONOCIDO", np.nan)
            .cast(pl.Float32, strict=True)
        )
        estancia = (pl.col("FECHAALTA") - pl.col("FECHA_INGRESO")).dt.total_days()
        anio = pl.col("FECHAALTA").dt.year()
        mes = pl.col("FECHAALTA").dt.month()
        fecha = pl.concat_str(anio.cast(str) + "-" + mes.cast(str))
        edad_persona = (
            (((pl.col("FECHAALTA") - pl.col("FECHA_NACIMIENTO")).dt.total_days()) / 365)
            .round(0)
            .cut(range(0, 121, 10))
        )
        prevision = pl.col("PREVISION").replace(REEMPLAZO_PREVISION, default=pl.first())

        df = df.with_columns(
            [
                peso_en_float.alias("IR_29301_PESO"),
                estancia.alias("ESTANCIA"),
                anio.alias("ANIO_EGRESO"),
                mes.alias("MES_EGRESO"),
                fecha.alias("FECHA"),
                edad_persona.alias("RANGO_ETARIO"),
                prevision.alias("PREVISION"),
            ]
        )
        df = df.collect()
        df = agregar_informacion_comuna(df)

        return df


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    # Lee GRD con una columna mas, y lo guarda eliminandola
    # grd_no_utf = leer_grd_no_utf8(input_filepath)
    # ruta_a_guardar_grd_no_utf = f"{input_filepath}/GRD_PUBLICO_CONCATENADO.txt"
    # grd_no_utf.to_csv(ruta_a_guardar_grd_no_utf, sep="|", index=False)

    df = leer_grd(input_filepath)
    # Filtra el torax
    df_hospital = df.filter(pl.col("COD_HOSPITAL") == "112103")

    df.write_csv(f"{output_filepath}/df_procesada.csv", separator=";")
    df_hospital.write_csv(f"{output_filepath}/df_procesada_112103.csv", separator=";")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
