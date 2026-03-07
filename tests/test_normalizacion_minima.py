import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

from pipeline.normailzacion import normalizacion as norm


@contextmanager
def temp_repo_layout():
    """Create a temporary repo-like layout expected by normalization functions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        previous_cwd = Path.cwd()
        os.chdir(root)
        try:
            yield root
        finally:
            os.chdir(previous_cwd)


class TestNormalizacionMinima(unittest.TestCase):
    def test_normalizar_fecha_iso(self):
        self.assertEqual(norm.normalizar_fecha_iso("2024/03/01"), "2024-03-01")
        self.assertEqual(norm.normalizar_fecha_iso("01-03-2024"), "2024-01-03")
        self.assertIsNone(norm.normalizar_fecha_iso(""))
        self.assertIsNone(norm.normalizar_fecha_iso("fecha invalida"))

    def test_xml_eu_to_df_columnas_estandar(self):
        eu_xml = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<export xmlns=\"http://eu.europa.ec/fpi/fsd/export\">
  <sanctionEntity designationDate=\"2024-01-15\" euReferenceNumber=\"EU-1\" logicalId=\"L-1\">
    <subjectType code=\"person\" />
    <regulation>
      <publicationUrl>Reg A</publicationUrl>
    </regulation>
    <nameAlias wholeName=\"DOE, JOHN\" firstName=\"JOHN\" middleName=\"\" lastName=\"DOE\" />
    <birthdate birthdate=\"1980-05-20\" countryIso2Code=\"US\" />
  </sanctionEntity>
  <sanctionEntity designationDate=\"2024-02-20\" euReferenceNumber=\"EU-2\" logicalId=\"L-2\">
    <subjectType code=\"enterprise\" />
    <regulation>
      <publicationUrl>Reg B</publicationUrl>
    </regulation>
    <nameAlias wholeName=\"ACME LTD\" firstName=\"ACME\" middleName=\"\" lastName=\"LTD\" />
  </sanctionEntity>
</export>
"""

        with temp_repo_layout() as repo_root:
            (repo_root / "data" / "raw" / "EU_DRIVE.xml").write_text(eu_xml, encoding="utf-8")
            df = norm.xml_eu_to_df()

        columnas_esperadas = [
            "fuente",
            "tipo_sujeto",
            "nombres",
            "apellidos",
            "aliases",
            "fecha_nacimiento",
            "nacionalidad",
            "numero_documento",
            "tipo_sancion",
            "fecha_sancion",
            "fecha_vencimiento",
            "activo",
            "id_referencia",
        ]

        # Some pipeline versions append technical columns (e.g. id_registro/hash)
        # before persistence. We only enforce the required normalized business schema.
        for col in columnas_esperadas:
            self.assertIn(col, df.columns)
        self.assertEqual(len(df), 2)

        persona = df[df["numero_documento"] == "EU-1"].iloc[0]
        juridica = df[df["numero_documento"] == "EU-2"].iloc[0]

        self.assertEqual(persona["tipo_sujeto"], "PERSONA_NATURAL")
        self.assertEqual(persona["fecha_sancion"], "2024-01-15")
        self.assertEqual(juridica["tipo_sujeto"], "PERSONA_JURIDICA")

    def test_json_fcpa_to_df_retorna_dataframe(self):
        fcpa_payload = {
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_source": {
                            "period_ending": "2024-03-31",
                            "file_date": "2024-04-15",
                            "form": "10-K",
                            "file_description": "Annual filing",
                            "ciks": ["123456"],
                            "display_names": ["Corp A"],
                            "biz_locations": ["Bogota"],
                            "inc_states": ["DE"],
                            "sics": [1000],
                            "biz_states": ["CA"],
                        },
                    }
                ]
            }
        }

        with temp_repo_layout() as repo_root:
            (repo_root / "data" / "raw" / "FCPA.json").write_text(
                json.dumps(fcpa_payload), encoding="utf-8"
            )
            df = norm.json_fcpa_to_df()

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertIn("fuente", df.columns)
        self.assertIn("numero_documento", df.columns)
        self.assertEqual(df.iloc[0]["fuente"], "FCPA")
        self.assertEqual(df.iloc[0]["fecha_sancion"], "2024-04-15")
        self.assertEqual(df.iloc[0]["periodo_finalizado"], "2024-03-31")


if __name__ == "__main__":
    unittest.main()
