"""Tests for ean_utils.py - EAN normalization, price parsing, ID construction."""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utilidades.ean_utils import (
    normalizar_ean,
    precio_a_float,
    parse_precio_argentino,
    construir_id,
    cargar_config_supermercados,
)


class TestNormalizarEan:
    """Tests for EAN normalization (zero-pad to 13 digits)."""

    def test_short_ean_pads_to_13(self):
        assert normalizar_ean("123") == "0000000000123"

    def test_ean_12_pads_to_13(self):
        assert normalizar_ean("123456789012") == "0123456789012"

    def test_ean_already_13(self):
        assert normalizar_ean("1234567890123") == "1234567890123"

    def test_ean_with_whitespace_strips(self):
        assert normalizar_ean("  123  ") == "0000000000123"

    def test_none_returns_none(self):
        assert normalizar_ean(None) is None

    def test_empty_string_returns_none(self):
        assert normalizar_ean("") is None

    def test_non_digit_returns_none(self):
        assert normalizar_ean("12a34") is None

    def test_ean_over_13_returns_none(self):
        assert normalizar_ean("12345678901234") is None

    def test_ean_with_dots_returns_none(self):
        assert normalizar_ean("12.345") is None


class TestPrecioAfloat:
    """Tests for precio_a_float - convert price strings to float."""

    def test_simple_integer(self):
        assert precio_a_float("1234") == 1234.0

    def test_with_commas(self):
        assert precio_a_float("1.234") == 1234.0

    def test_with_commas_and_decimals(self):
        assert precio_a_float("1.234,56") == 1234.56

    def test_argentine_format(self):
        assert precio_a_float("$ 1.234,56") == 1234.56

    def test_argentine_format_no_space(self):
        assert precio_a_float("$1.234,56") == 1234.56

    def test_decimal_only(self):
        assert precio_a_float("0,99") == 0.99

    def test_negative_price(self):
        assert precio_a_float("-123,45") == -123.45

    def test_none_returns_none(self):
        assert precio_a_float(None) is None

    def test_empty_returns_none(self):
        assert precio_a_float("") is None

    def test_invalid_returns_none(self):
        assert precio_a_float("not_a_price") is None


class TestParsePrecioArgentino:
    """Tests for parse_precio_argentino - parse Argentine-formatted prices."""

    def test_standard_format(self):
        assert parse_precio_argentino("$ 1.234,56") == 1234.56

    def test_no_thousands_separator(self):
        assert parse_precio_argentino("$ 1234,56") == 1234.56

    def test_integer_price(self):
        assert parse_precio_argentino("$ 1.234") == 1234.0

    def test_no_currency_symbol(self):
        assert parse_precio_argentino("1.234,56") == 1234.56

    def test_none_returns_none(self):
        assert parse_precio_argentino(None) is None

    def test_empty_returns_none(self):
        assert parse_precio_argentino("") is None

    def test_invalid_returns_none(self):
        assert parse_precio_argentino("abc") is None


class TestConstruirId:
    """Tests for construir_id - build composite supermarket-product ID."""

    def test_standard_case(self):
        assert construir_id("02", "123456789012") == "02-0123456789012"

    def test_none_ean_returns_none(self):
        assert construir_id("02", None) is None

    def test_invalid_ean_returns_none(self):
        assert construir_id("02", "invalid") is None


class TestCargarConfig:
    """Tests for cargar_config_supermercados - load supermarket config."""

    def test_returns_dict_with_expected_keys(self):
        config = cargar_config_supermercados()
        assert isinstance(config, dict)
        assert "01" in config
        assert "02" in config
        assert "03" in config
        assert "04" in config

    def test_supermarket_names(self):
        config = cargar_config_supermercados()
        assert config["01"] == "Cooperativa Obrera"
        assert config["02"] == "Vea"
        assert config["03"] == "ChangoMas"
        assert config["04"] == "La Banderita"
