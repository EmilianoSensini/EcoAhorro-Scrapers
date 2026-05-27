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
    return f"{ean}{id_supermercado}"


def precio_a_float(valor) -> float | None:
    if isinstance(valor, (int, float)):
        return float(valor)
    if not isinstance(valor, str) or not valor.strip():
        return None
    s = valor.replace("$", "").replace(" ", "").strip()
    if not s:
        return None

    # Argentine format: decimal comma, optional dot as thousands separator
    # e.g. "1.234,56" or "1234,56" or "1.234" or "5,89"
    if re.search(r"\d+,\d{2}$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
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
