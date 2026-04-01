from enum import Enum
import pyspark.sql.functions as F
from pyspark.sql import Column


class AdmLevel(Enum):
    ADM0 = "ADM0"
    ADM1 = "ADM1"
    ADM2 = "ADM2"
    ADM3 = "ADM3"
    ADM4 = "ADM4"


class Lang(Enum):
    EN = "EN"
    UK = "UK"


def clean_municipality_col(c: Column) -> Column:
    """
    Pure PySpark column expression equivalent of Monument.cleanMunicipality in Scala.
    Applies a series of regexp_replace operations then extracts the part before
    the first '(' or '|' and trims whitespace.
    """
    # Each tuple is (regex_pattern, replacement).
    # Literal strings with regex special characters are escaped.
    replacements = [
        ("р-н", "район"),
        ("сільська рада", ""),
        ("селищна рада", ""),
        ("\\[\\[", ""),
        ("\\]\\]", ""),
        ("&nbsp;", " "),
        ("\u00A0", " "),    # non-breaking space
        ("м\\.", ""),
        ("місто", ""),
        ("с\\.", ""),
        ("С\\.", ""),
        ("\\.", ""),         # remove all remaining dots
        ("село", ""),
        ("смт", ""),
        ("Смт", ""),
        ("с-ще", ""),
        ("с-щ", ""),
        ("'''", ""),
        ("''", ""),
        (",", ""),
        ("\u2019", "'"),    # right single quotation mark → apostrophe
        ("\u201c", "'"),    # left double quotation mark → apostrophe
    ]
    result = c
    for pattern, replacement in replacements:
        result = F.regexp_replace(result, pattern, replacement)
    # Mirror Scala's .split("\\(").head.split("\\|").head.trim:
    # capture everything before the first '(' or '|', then trim.
    result = F.trim(F.regexp_extract(result, r"^([^(|]*)", 1))
    return result
