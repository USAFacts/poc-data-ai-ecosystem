"""Census Bureau Data API models — ported from uscensusbureau/us-census-bureau-data-api-mcp."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.domain import Base


class CensusSummaryLevelModel(Base):
    """Geographic hierarchy level (State, County, Tract, Place, etc.)."""

    __tablename__ = "census_summary_levels"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(3), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    get_variable: Mapped[str | None] = mapped_column(String(100))
    query_name: Mapped[str | None] = mapped_column(String(100))
    on_spine: Mapped[bool] = mapped_column(Boolean, default=False)
    hierarchy_level: Mapped[int] = mapped_column(Integer, default=0)
    parent_summary_level: Mapped[str | None] = mapped_column(
        String(3), ForeignKey("census_summary_levels.code")
    )

    geographies: Mapped[list["CensusGeographyModel"]] = relationship(
        back_populates="summary_level", lazy="selectin"
    )


class CensusGeographyModel(Base):
    """Geography with FIPS code and pre-computed Census API for/in params."""

    __tablename__ = "census_geographies"
    __table_args__ = (UniqueConstraint("fips_code", "year", name="uq_census_geo_fips_year"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    full_name: Mapped[str | None] = mapped_column(String(1000))
    state_code: Mapped[str | None] = mapped_column(String(2))
    county_code: Mapped[str | None] = mapped_column(String(3))
    fips_code: Mapped[str | None] = mapped_column(String(20))
    census_geoid: Mapped[str | None] = mapped_column(String(40))
    ucgid_code: Mapped[str | None] = mapped_column(String(60), unique=True)
    summary_level_code: Mapped[str | None] = mapped_column(
        String(3), ForeignKey("census_summary_levels.code")
    )
    for_param: Mapped[str] = mapped_column(String(255), nullable=False)
    in_param: Mapped[str | None] = mapped_column(String(255))
    latitude: Mapped[float | None] = mapped_column()
    longitude: Mapped[float | None] = mapped_column()
    population: Mapped[int | None] = mapped_column(BigInteger)
    land_area_sqkm: Mapped[float | None] = mapped_column()
    region_code: Mapped[str | None] = mapped_column(String(5))
    division_code: Mapped[str | None] = mapped_column(String(5))
    place_code: Mapped[str | None] = mapped_column(String(10))
    year: Mapped[int] = mapped_column(Integer, default=2023)

    summary_level: Mapped[CensusSummaryLevelModel | None] = relationship(
        back_populates="geographies", lazy="selectin"
    )


class CensusProgramModel(Base):
    """Top-level survey program (e.g. American Community Survey)."""

    __tablename__ = "census_programs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    acronym: Mapped[str | None] = mapped_column(String(50), unique=True)

    components: Mapped[list["CensusComponentModel"]] = relationship(
        back_populates="program", lazy="selectin"
    )


class CensusComponentModel(Base):
    """Survey component (e.g. ACS 1-Year Detailed Tables)."""

    __tablename__ = "census_components"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(500), nullable=False)
    component_id: Mapped[str | None] = mapped_column(String(100), unique=True)
    api_endpoint: Mapped[str | None] = mapped_column(String(255), unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    program_id: Mapped[int | None] = mapped_column(ForeignKey("census_programs.id"))

    program: Mapped[CensusProgramModel | None] = relationship(back_populates="components")
    datasets: Mapped[list["CensusDatasetModel"]] = relationship(
        back_populates="component", lazy="selectin"
    )


class CensusDatasetModel(Base):
    """Census dataset for a specific year and endpoint."""

    __tablename__ = "census_datasets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str | None] = mapped_column(String(500))
    api_endpoint: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(String(50), default="aggregate")
    year: Mapped[int | None] = mapped_column(Integer)
    component_id: Mapped[int | None] = mapped_column(ForeignKey("census_components.id"))

    component: Mapped[CensusComponentModel | None] = relationship(back_populates="datasets")


class CensusDataTableModel(Base):
    """Census table catalog entry (e.g. B01001, S0101)."""

    __tablename__ = "census_data_tables"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data_table_id: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    label: Mapped[str | None] = mapped_column(Text)


class CensusDataTableDatasetModel(Base):
    """Junction: which data tables are available in which datasets."""

    __tablename__ = "census_data_table_datasets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey("census_datasets.id"), nullable=False)
    data_table_id: Mapped[int] = mapped_column(ForeignKey("census_data_tables.id"), nullable=False)
    label: Mapped[str | None] = mapped_column(Text)


class CensusDataCacheModel(Base):
    """Cache for Census API responses to avoid repeated calls."""

    __tablename__ = "census_data_cache"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    dataset_code: Mapped[str | None] = mapped_column(String(255))
    year: Mapped[int | None] = mapped_column(Integer)
    variables: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    geography_spec: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    response_data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
