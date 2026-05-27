"""Tests for consolidar_precios.py - merges individual scraper CSVs."""

import os
import csv
import pytest

import etl.step_consolidate as cp_mod


@pytest.fixture
def temp_prices_dir(tmp_path, monkeypatch):
    prices_dir = tmp_path / "data" / "prices"
    prices_dir.mkdir(parents=True)
    output_file = tmp_path / "data" / "consolidado_precios.csv"
    monkeypatch.setattr(cp_mod, "CARPETA_PRECIOS", str(prices_dir))
    monkeypatch.setattr(cp_mod, "ARCHIVO_SALIDA", str(output_file))
    return prices_dir, output_file


def _write_price_csv(directory, filename, rows):
    path = os.path.join(str(directory), filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "idProducto", "idSupermercado", "precio", "actualizacion"])
        for row in rows:
            w.writerow(row)


class TestConsolidar:
    """Tests for consolidar_precios.consolidar()."""

    def test_merges_multiple_files(self, temp_prices_dir):
        prices_dir, output_file = temp_prices_dir
        _write_price_csv(prices_dir, "precios_01_20260514.csv", [
            ["01-123", "123", "01", "1234.56", "2026-05-14"],
        ])
        _write_price_csv(prices_dir, "precios_02_20260514.csv", [
            ["02-456", "456", "02", "789.00", "2026-05-14"],
        ])
        stats = cp_mod.consolidar()
        assert stats["archivos"] == 2
        assert stats["registros"] == 2
        with open(str(output_file), "r", encoding="utf-8") as f:
            lines = list(csv.reader(f))
        assert len(lines) == 3

    def test_skips_files_with_wrong_headers(self, temp_prices_dir):
        prices_dir, output_file = temp_prices_dir
        bad_file = os.path.join(str(prices_dir), "precios_03.csv")
        with open(bad_file, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["wrong", "columns"])
            w.writerow(["a", "b"])
        _write_price_csv(prices_dir, "precios_01_20260514.csv", [
            ["01-123", "123", "01", "1234.56", "2026-05-14"],
        ])
        stats = cp_mod.consolidar()
        assert stats["archivos"] == 1
        assert stats["registros"] == 1

    def test_empty_dir_returns_zero_stats(self, temp_prices_dir):
        stats = cp_mod.consolidar()
        assert stats["archivos"] == 0
        assert stats["registros"] == 0
