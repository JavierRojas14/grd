# -*- coding: utf-8 -*-
import logging
from pathlib import Path

import click
from dotenv import find_dotenv, load_dotenv

import polars as pl
import pandas as pd


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
}


def agregar_informacion_comuna(df):
    tmp = df.clone()

    comunas = (
        pd.read_excel("data/external/Esquema_Registro-2023.xls", sheet_name=2, header=6).dropna(
            how="all", axis=1
        )
    ).dropna()

    comunas = pl.from_dataframe(comunas)

    tmp = tmp.join(comunas, how="inner", left_on="COMUNA", right_on="Nombre Comuna")
    return tmp


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
            dtypes=COLUMNAS_POLARS,
            null_values=VALORES_NULOS_COLUMNAS,
        )

        peso_en_float = pl.col("IR_29301_PESO").str.replace(",", ".").cast(pl.Float32, strict=True)
        estancia = (pl.col("FECHAALTA") - pl.col("FECHA_INGRESO")).dt.days()
        anio = pl.col("FECHAALTA").dt.year()
        mes = pl.col("FECHAALTA").dt.month()
        fecha = pl.concat_str(anio.cast(str) + "-" + mes.cast(str))
        edad_persona = (
            (((pl.col("FECHAALTA") - pl.col("FECHA_NACIMIENTO")).dt.days()) / 365)
            .round(0)
            .cut(range(0, 121, 10))
        )

        df = df.with_columns(
            [
                peso_en_float.alias("IR_29301_PESO"),
                estancia.alias("ESTANCIA"),
                anio.alias("ANIO_EGRESO"),
                mes.alias("MES_EGRESO"),
                fecha.alias("FECHA"),
                edad_persona.alias("RANGO_ETARIO"),
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

    df = leer_grd(input_filepath)
    df.write_csv(f"{output_filepath}/df_procesada.csv", separator=";")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
