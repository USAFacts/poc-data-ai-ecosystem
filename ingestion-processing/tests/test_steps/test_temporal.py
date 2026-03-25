"""Tests for temporal URL resolution."""

from datetime import date

import pytest

from pipeline.control.models import TemporalConfig, TemporalPattern
from pipeline.steps.acquisition.temporal import (
    get_fiscal_year_quarter,
    resolve_temporal_context,
    resolve_temporal_url,
    get_previous_period,
)


class TestFiscalYearQuarter:
    """Tests for fiscal year/quarter calculation."""

    def test_q1_october(self) -> None:
        """October is Q1 of next fiscal year."""
        fy, q = get_fiscal_year_quarter(date(2024, 10, 15))
        assert fy == 2025
        assert q == 1

    def test_q1_december(self) -> None:
        """December is Q1."""
        fy, q = get_fiscal_year_quarter(date(2024, 12, 1))
        assert fy == 2025
        assert q == 1

    def test_q2_january(self) -> None:
        """January is Q2."""
        fy, q = get_fiscal_year_quarter(date(2025, 1, 15))
        assert fy == 2025
        assert q == 2

    def test_q2_march(self) -> None:
        """March is Q2."""
        fy, q = get_fiscal_year_quarter(date(2025, 3, 31))
        assert fy == 2025
        assert q == 2

    def test_q3_april(self) -> None:
        """April is Q3."""
        fy, q = get_fiscal_year_quarter(date(2025, 4, 1))
        assert fy == 2025
        assert q == 3

    def test_q3_june(self) -> None:
        """June is Q3."""
        fy, q = get_fiscal_year_quarter(date(2025, 6, 30))
        assert fy == 2025
        assert q == 3

    def test_q4_july(self) -> None:
        """July is Q4."""
        fy, q = get_fiscal_year_quarter(date(2025, 7, 1))
        assert fy == 2025
        assert q == 4

    def test_q4_september(self) -> None:
        """September is Q4 (last month of fiscal year)."""
        fy, q = get_fiscal_year_quarter(date(2025, 9, 30))
        assert fy == 2025
        assert q == 4


class TestTemporalUrlResolution:
    """Tests for URL template resolution."""

    def test_uscis_pattern(self) -> None:
        """Test USCIS quarterly forms URL pattern."""
        config = TemporalConfig(
            pattern=TemporalPattern.FISCAL_YEAR_QUARTER,
            fiscalYearStartMonth=10,
            urlTemplate="https://www.uscis.gov/sites/default/files/document/data/quarterly_all_forms_fy{fiscal_year}_q{quarter}.xlsx",
        )

        # Test for Q3 FY2025 (April-June 2025)
        url = resolve_temporal_url(config, date(2025, 5, 15))
        assert url == "https://www.uscis.gov/sites/default/files/document/data/quarterly_all_forms_fy2025_q3.xlsx"

    def test_short_aliases(self) -> None:
        """Test short aliases {fy} and {q}."""
        config = TemporalConfig(
            pattern=TemporalPattern.FISCAL_YEAR_QUARTER,
            fiscalYearStartMonth=10,
            urlTemplate="https://example.gov/data_fy{fy}_q{q}.csv",
        )

        url = resolve_temporal_url(config, date(2025, 1, 15))  # Q2 FY2025
        assert url == "https://example.gov/data_fy2025_q2.csv"

    def test_calendar_year_pattern(self) -> None:
        """Test calendar year/quarter pattern."""
        config = TemporalConfig(
            pattern=TemporalPattern.CALENDAR_YEAR_QUARTER,
            urlTemplate="https://example.gov/data_{year}_q{quarter}.csv",
        )

        url = resolve_temporal_url(config, date(2025, 5, 15))  # Calendar Q2
        assert url == "https://example.gov/data_2025_q2.csv"

    def test_calendar_year_month(self) -> None:
        """Test calendar year/month pattern."""
        config = TemporalConfig(
            pattern=TemporalPattern.CALENDAR_YEAR_MONTH,
            urlTemplate="https://example.gov/data_{year}_{month}.csv",
        )

        url = resolve_temporal_url(config, date(2025, 3, 15))
        assert url == "https://example.gov/data_2025_03.csv"


class TestPreviousPeriod:
    """Tests for getting previous period."""

    def test_previous_quarter_mid_year(self) -> None:
        """Test getting previous quarter mid-year."""
        config = TemporalConfig(
            pattern=TemporalPattern.FISCAL_YEAR_QUARTER,
            fiscalYearStartMonth=10,
            urlTemplate="https://example.gov/fy{fiscal_year}_q{quarter}.csv",
        )

        # Current: Q3 FY2025 -> Previous: Q2 FY2025
        prev = get_previous_period(config, date(2025, 5, 15))
        assert prev.fiscal_year == 2025
        assert prev.quarter == 2

    def test_previous_quarter_year_boundary(self) -> None:
        """Test getting previous quarter across year boundary."""
        config = TemporalConfig(
            pattern=TemporalPattern.FISCAL_YEAR_QUARTER,
            fiscalYearStartMonth=10,
            urlTemplate="https://example.gov/fy{fiscal_year}_q{quarter}.csv",
        )

        # Current: Q1 FY2025 (Oct 2024) -> Previous: Q4 FY2024
        prev = get_previous_period(config, date(2024, 10, 15))
        assert prev.fiscal_year == 2024
        assert prev.quarter == 4
