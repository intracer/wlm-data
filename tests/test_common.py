from wlm.common import AdmLevel, Lang


def test_adm_level_values():
    assert AdmLevel.ADM0.value == "ADM0"
    assert AdmLevel.ADM1.value == "ADM1"
    assert AdmLevel.ADM2.value == "ADM2"
    assert AdmLevel.ADM3.value == "ADM3"
    assert AdmLevel.ADM4.value == "ADM4"


def test_lang_values():
    assert Lang.EN.value == "EN"
    assert Lang.UK.value == "UK"
