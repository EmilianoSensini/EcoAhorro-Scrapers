"""Tests for normalizar_catalogo.py - EAN normalization and dedup in product catalog."""

import sys
import os
import csv
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utilidades.normalizar_catalogo import normalizar_id, procesar


class TestNormalizarId:
    """Tests for normalizar_id - individual EAN normalization."""

    def test_pads_short(self):
        assert normalizar_id("123") == "0000000000123"

    def test_pads_12_digits(self):
        assert normalizar_id("123456789012") == "0123456789012"

    def test_keeps_13_digits(self):
        assert normalizar_id("1234567890123") == "1234567890123"

    def test_strips_whitespace(self):
        assert normalizar_id("  123  ") == "0000000000123"

    def test_strips_quotes(self):
        assert normalizar_id('"123"') == "0000000000123"

    def test_empty_returns_none(self):
        assert normalizar_id("") is None

    def test_non_digit_returns_none(self):
        assert normalizar_id("12a34") is None

    def test_over_13_after_padding_returns_none(self):
        assert normalizar_id("12345678901234") is None


class TestProcesar:
    """Tests for procesar - full catalog normalization pipeline."""

    @pytest.fixture
    def sample_catalog(self, tmp_path):
        input_file = tmp_path / "catalogo_input.csv"
        with open(str(input_file), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ID/EAN", "nombre", "marca", "presentacion", "categoria"])
            w.writerow(["123", "Producto A", "Marca X", "1L", "Lacteos"])
            w.writerow(["0123456789012", "Producto B", "Marca Y", "500g", "Almacen"])
            w.writerow(["", "Producto C", "Marca Z", "200g", "Frescos"])
            w.writerow(["abcdefghijklm", "Producto D", "Marca W", "300g", "Bebidas"])
            w.writerow(["1234567890123", "Producto E", "Marca V", "400g", "Limpieza"])
        output_file = tmp_path / "catalogo_normalizado.csv"
        return str(input_file), str(output_file)

    def test_pads_short_eans(self, sample_catalog):
        input_file, output_file = sample_catalog
        procesar(input_file, output_file)
        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        prod_a = [r for r in rows if r["nombre"] == "Producto A"]
        assert len(prod_a) == 1
        assert prod_a[0]["ID/EAN"] == "0000000000123"

    def test_removes_invalid_eans(self, sample_catalog):
        input_file, output_file = sample_catalog
        procesar(input_file, output_file)
        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        names = [r["nombre"] for r in rows]
        assert "Producto C" not in names
        assert "Producto D" not in names

    def test_removes_duplicates(self, tmp_path):
        input_file = tmp_path / "catalogo_dups.csv"
        with open(str(input_file), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ID/EAN", "nombre", "marca", "presentacion", "categoria"])
            w.writerow(["123", "Producto A", "Marca X", "1L", "Lacteos"])
            w.writerow(["0000000000123", "Producto A Dup", "Marca X", "1L", "Lacteos"])
        output_file = tmp_path / "catalogo_dedup.csv"
        stats = procesar(str(input_file), str(output_file))
        assert stats["duplicados"] == 1
        assert stats["escritos"] == 1

    def test_returns_stats(self, sample_catalog):
        input_file, output_file = sample_catalog
        stats = procesar(input_file, output_file)
        assert stats["leidos"] == 5
        assert stats["invalidos"] == 2
        assert stats["escritos"] == 3
