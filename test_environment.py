import sys

import pandas as pd
import unittest

from src.features.build_features import contar_con_agrupacion

REQUIRED_PYTHON = "python3"


class TestDataFrameTransformationAndContarConAgrupacion(unittest.TestCase):
    def test_dataframe_transformation(self):
        # Create a sample wide-format DataFrame for testing
        data = {
            "COD_HOSPITAL": [1, 2, 3],
            "ANIO_EGRESO": [2020, 2021, 2022],
            "TIPO_ACTIVIDAD": ["A", "B", "C"],
            "CIP_ENCRIPTADO": ["X", "Y", "Z"],
            "TIPOALTA": ["High", "Medium", "Low"],
            "DIAGNOSTICO1": ["D1", "D2", "D3"],
            "IR_29301_PESO": [50, 60, 70],
            "IR_29301_SEVERIDAD": [1, 2, 3],
            "PROCEDIMIENTO1": ["P1A", "P1B", "P1C"],
            "PROCEDIMIENTO2": ["P2A", "P2B", "P2C"],
        }
        df_procesada = pd.DataFrame(data)

        # Perform the transformation
        procedimientos_3_long = pd.melt(
            df_procesada,
            id_vars=[
                "COD_HOSPITAL",
                "ANIO_EGRESO",
                "TIPO_ACTIVIDAD",
                "CIP_ENCRIPTADO",
                "TIPOALTA",
                "DIAGNOSTICO1",
                "IR_29301_PESO",
                "IR_29301_SEVERIDAD",
            ],
            value_vars=[f"PROCEDIMIENTO{i}" for i in range(1, 3)],
            value_name="procedimiento",
        )

        # Define the expected long-format DataFrame
        expected_data = {
            "COD_HOSPITAL": [1, 2, 3, 1, 2, 3],
            "ANIO_EGRESO": [2020, 2021, 2022, 2020, 2021, 2022],
            "TIPO_ACTIVIDAD": ["A", "B", "C", "A", "B", "C"],
            "CIP_ENCRIPTADO": ["X", "Y", "Z", "X", "Y", "Z"],
            "TIPOALTA": ["High", "Medium", "Low", "High", "Medium", "Low"],
            "DIAGNOSTICO1": ["D1", "D2", "D3", "D1", "D2", "D3"],
            "IR_29301_PESO": [50, 60, 70, 50, 60, 70],
            "IR_29301_SEVERIDAD": [1, 2, 3, 1, 2, 3],
            "variable": [
                "PROCEDIMIENTO1",
                "PROCEDIMIENTO1",
                "PROCEDIMIENTO1",
                "PROCEDIMIENTO2",
                "PROCEDIMIENTO2",
                "PROCEDIMIENTO2",
            ],
            "procedimiento": ["P1A", "P1B", "P1C", "P2A", "P2B", "P2C"],
        }
        expected_df = pd.DataFrame(expected_data)

        # Check if the transformed DataFrame matches the expected DataFrame
        pd.testing.assert_frame_equal(procedimientos_3_long, expected_df)

    def test_contar_con_agrupacion(self):
        # Create a sample DataFrame for testing
        data = {
            "COD_HOSPITAL": [1, 2, 2, 3, 1],
            "TIPO_ACTIVIDAD": ["A", "B", "A", "C", "A"],
            "procedimiento": ["P1A", "P1B", "P1A", "P2A", "P1A"],
        }
        df = pd.DataFrame(data)

        # Call the contar_con_agrupacion function
        conteo_agrupado = contar_con_agrupacion(
            df, ["COD_HOSPITAL", "TIPO_ACTIVIDAD"], "procedimiento", "conteo"
        )

        # Define the expected result DataFrame
        expected_data = {
            "COD_HOSPITAL": [1, 2, 2, 3],
            "TIPO_ACTIVIDAD": ["A", "A", "B", "C"],
            "procedimiento": ["P1A", "P1A", "P1B", "P2A"],
            "conteo": [2, 1, 1, 1],
        }
        expected_df = pd.DataFrame(expected_data)

        # Check if the result matches the expected DataFrame
        pd.testing.assert_frame_equal(conteo_agrupado, expected_df)


def main():
    system_major = sys.version_info.major
    if REQUIRED_PYTHON == "python":
        required_major = 2
    elif REQUIRED_PYTHON == "python3":
        required_major = 3
    else:
        raise ValueError("Unrecognized python interpreter: {}".format(REQUIRED_PYTHON))

    if system_major != required_major:
        raise TypeError(
            "This project requires Python {}. Found: Python {}".format(required_major, sys.version)
        )
    else:
        print(">>> Development environment passes all tests!")


if __name__ == "__main__":
    unittest.main()
    main()
