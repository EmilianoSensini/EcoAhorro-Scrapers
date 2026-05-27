"""Tests for tabla_maestra.py - filters consolidated prices against normalized catalog."""

import sys
import os
import csv
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import utilidades.tabla_maestra as tm_mod


@pytest.fixture
def temp_data_dir(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    consolidated = data_dir / "consolidado_precios.csv"
    with open(str(consolidated), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])
        w.writerow(["01-0000000000123", "0000000000123", "01", "1234.56", "2026-05-14"])
        w.writerow(["02-0000000000123", "0000000000123", "02", "1200.00", "2026-05-14"])
        w.writerow(["01-0000000000456", "0000000000456", "01", "789.00", "2026-05-14"])
        w.writerow(["03-0000000000789", "0000000000789", "03", "500.00", "2026-05-14"])

    catalog = data_dir / "catalogo_productos_normalizado.csv"
    with open(str(catalog), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ID/EAN", "nombre", "marca", "presentacion", "categoria"])
        w.writerow(["0000000000123", "Producto A", "Marca X", "1L", "Lacteos"])
        w.writerow(["0000000000456", "Producto B", "Marca Y", "500g", "Almacen"])

    output_file = data_dir / "tabla_maestra.csv"
    monkeypatch.setattr(tm_mod, "ARCHIVO_CONSOLIDADO", str(consolidated))
    monkeypatch.setattr(tm_mod, "ARCHIVO_CATALOGO", str(catalog))
    monkeypatch.setattr(tm_mod, "ARCHIVO_SALIDA", str(output_file))
    return consolidated, catalog, output_file


class TestProcesar:
    """Tests for tabla_maestra.procesar()."""

    def test_filters_unknown_products(self, temp_data_dir):
        _, _, output_file = temp_data_dir
        stats = tm_mod.procesar()
        assert stats["no_catalogo"] == 1
        assert stats["catalogo_validos"] == 3
        with open(str(output_file), "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        eans = [r["idProducto"] for r in rows]
        assert "0000000000789" not in eans

    def test_deduplicates_by_newest(self, temp_data_dir):
        _, _, output_file = temp_data_dir
        stats = tm_mod.procesar()
        assert stats["unicos_final"] == 3

    def test_output_columns(self, temp_data_dir):
        _, _, output_file = temp_data_dir
        tm_mod.procesar()
        with open(str(output_file), "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        expected = {"id", "idProducto", "idSupermercado", "precio", "actualizacion"}
        assert rows[0].keys() == expected

    def test_returns_stats_dict(self, temp_data_dir):
        stats = tm_mod.procesar()
        assert "leidos" in stats
        assert "no_catalogo" in stats
        assert "catalogo_validos" in stats
        assert "unicos_final" in stats
