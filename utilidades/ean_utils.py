import re
import json
import os

LONGITUD_EAN = 13


def cargar_config_supermercados() -> dict:
    ruta = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "supermercados_config.json",
    )
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def normalizar_ean(id_raw: str) -> str | None:
    if id_raw is None:
        return None
    id_str = id_raw.strip().strip('"').strip("'")
    if not id_str:
        return None
    id_str = id_str.zfill(LONGITUD_EAN)
    if len(id_str) == LONGITUD_EAN and id_str.isdigit():
        return id_str
    return None


def construir_id(id_supermercado: str, id_producto: str) -> str:
    ean = normalizar_ean(id_producto)
    if ean is None:
        return None
    return f"{id_supermercado}-{ean}"


def precio_a_float(valor) -> float:
    if isinstance(valor, (int, float)):
        return float(valor)
    if not isinstance(valor, str) or not valor.strip():
        return None
    s = valor.replace("$", "").replace(" ", "").strip()
    s = s.replace(".", "").replace(",", ".")
    m = re.search(r"-?[\d.]+", s)
    if m:
        try:
            return float(m.group())
        except ValueError:
            return None
    return None


def parse_precio_argentino(valor) -> float:
    if isinstance(valor, (int, float)):
        return float(valor)
    if not isinstance(valor, str) or not valor.strip():
        return None
    s = valor.replace("$", "").replace(" ", "").strip()
    if not s:
        return None
    # Detect if there is a decimal comma (Argentine format)
    if re.search(r"\d+,\d{2}$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        # No decimal comma: treat dots as thousands separators if more than one
        # or if followed by exactly 3 digits at the end
        if re.search(r"\d\.\d{3}$", s):
            s = s.replace(".", "")
        s = s.replace(",", ".")
    m = re.search(r"-?[\d.]+", s)
    if m:
        try:
            return float(m.group())
        except ValueError:
            return None
    return None
