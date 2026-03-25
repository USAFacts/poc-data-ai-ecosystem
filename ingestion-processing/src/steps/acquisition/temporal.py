"""Temporal URL resolution for predictable enumeration patterns."""

from dataclasses import dataclass
from datetime import date, datetime

from control.models import TemporalConfig, TemporalPattern


@dataclass
class TemporalContext:
    """Resolved temporal values for URL template substitution."""

    fiscal_year: int
    quarter: int
    year: int
    month: int

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for URL template substitution."""
        return {
            "fiscal_year": str(self.fiscal_year),
            "fy": str(self.fiscal_year),  # Short alias
            "quarter": str(self.quarter),
            "q": str(self.quarter),  # Short alias
            "year": str(self.year),
            "month": str(self.month).zfill(2),
            "month_num": str(self.month),
        }


def get_fiscal_year_quarter(
    reference_date: date, fiscal_year_start_month: int = 10
) -> tuple[int, int]:
    """Calculate fiscal year and quarter for a given date.

    US Federal fiscal year starts October 1:
    - FY2025 runs Oct 1, 2024 to Sep 30, 2025
    - Q1 = Oct-Dec, Q2 = Jan-Mar, Q3 = Apr-Jun, Q4 = Jul-Sep

    Args:
        reference_date: Date to calculate fiscal year/quarter for
        fiscal_year_start_month: Month when fiscal year starts (1-12, default 10 for October)

    Returns:
        Tuple of (fiscal_year, quarter)
    """
    year = reference_date.year
    month = reference_date.month

    # Calculate fiscal year
    if month >= fiscal_year_start_month:
        fiscal_year = year + 1
    else:
        fiscal_year = year

    # Calculate fiscal quarter (1-indexed)
    # Months since fiscal year start
    months_into_fy = (month - fiscal_year_start_month) % 12
    quarter = (months_into_fy // 3) + 1

    return fiscal_year, quarter


def get_calendar_quarter(reference_date: date) -> int:
    """Get calendar quarter (1-4) for a date."""
    return (reference_date.month - 1) // 3 + 1


def resolve_temporal_context(
    config: TemporalConfig,
    reference_date: date | None = None,
) -> TemporalContext:
    """Resolve temporal values based on pattern and reference date.

    Args:
        config: Temporal configuration from asset
        reference_date: Date to resolve for (default: today)

    Returns:
        TemporalContext with resolved values
    """
    if reference_date is None:
        reference_date = date.today()

    year = reference_date.year
    month = reference_date.month

    if config.pattern == TemporalPattern.FISCAL_YEAR_QUARTER:
        fiscal_year, quarter = get_fiscal_year_quarter(
            reference_date, config.fiscal_year_start_month
        )
    elif config.pattern == TemporalPattern.CALENDAR_YEAR_QUARTER:
        fiscal_year = year  # Use calendar year
        quarter = get_calendar_quarter(reference_date)
    elif config.pattern == TemporalPattern.CALENDAR_YEAR_MONTH:
        fiscal_year = year
        quarter = get_calendar_quarter(reference_date)
    elif config.pattern == TemporalPattern.CALENDAR_YEAR:
        fiscal_year = year
        quarter = get_calendar_quarter(reference_date)
    else:
        raise ValueError(f"Unknown temporal pattern: {config.pattern}")

    return TemporalContext(
        fiscal_year=fiscal_year,
        quarter=quarter,
        year=year,
        month=month,
    )


def resolve_temporal_url(
    config: TemporalConfig,
    reference_date: date | None = None,
) -> str:
    """Resolve a temporal URL template to a concrete URL.

    Args:
        config: Temporal configuration with URL template
        reference_date: Date to resolve for (default: today)

    Returns:
        Resolved URL string

    Example:
        Template: "https://example.gov/data_fy{fiscal_year}_q{quarter}.xlsx"
        Result: "https://example.gov/data_fy2025_q3.xlsx"
    """
    context = resolve_temporal_context(config, reference_date)
    url = config.url_template

    # Substitute all template variables
    for key, value in context.to_dict().items():
        url = url.replace(f"{{{key}}}", value)

    return url


def get_previous_period(
    config: TemporalConfig,
    reference_date: date | None = None,
) -> TemporalContext:
    """Get the previous period's temporal context.

    Useful for fetching the most recently completed period
    (since current period data may not yet be available).

    Args:
        config: Temporal configuration
        reference_date: Date to calculate from (default: today)

    Returns:
        TemporalContext for the previous period
    """
    if reference_date is None:
        reference_date = date.today()

    current = resolve_temporal_context(config, reference_date)

    if config.pattern in (TemporalPattern.FISCAL_YEAR_QUARTER, TemporalPattern.CALENDAR_YEAR_QUARTER):
        # Go back one quarter
        if current.quarter == 1:
            return TemporalContext(
                fiscal_year=current.fiscal_year - 1,
                quarter=4,
                year=current.year if current.month > 3 else current.year - 1,
                month=current.month,
            )
        else:
            return TemporalContext(
                fiscal_year=current.fiscal_year,
                quarter=current.quarter - 1,
                year=current.year,
                month=current.month,
            )
    elif config.pattern == TemporalPattern.CALENDAR_YEAR_MONTH:
        # Go back one month
        if current.month == 1:
            return TemporalContext(
                fiscal_year=current.year - 1,
                quarter=4,
                year=current.year - 1,
                month=12,
            )
        else:
            new_month = current.month - 1
            return TemporalContext(
                fiscal_year=current.fiscal_year,
                quarter=(new_month - 1) // 3 + 1,
                year=current.year,
                month=new_month,
            )
    else:
        # Calendar year - go back one year
        return TemporalContext(
            fiscal_year=current.fiscal_year - 1,
            quarter=current.quarter,
            year=current.year - 1,
            month=current.month,
        )


def resolve_temporal_url_previous(
    config: TemporalConfig,
    reference_date: date | None = None,
) -> str:
    """Resolve URL for the previous period (most recently completed).

    Args:
        config: Temporal configuration with URL template
        reference_date: Date to calculate from (default: today)

    Returns:
        Resolved URL for previous period
    """
    prev_context = get_previous_period(config, reference_date)
    url = config.url_template

    for key, value in prev_context.to_dict().items():
        url = url.replace(f"{{{key}}}", value)

    return url
