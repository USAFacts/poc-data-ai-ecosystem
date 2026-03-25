"""LLM prompt templates for metadata suggestion.

Contains the system prompts and tool definitions used for
generating metadata suggestions via Claude.
"""

# System prompt for table metadata suggestion
TABLE_METADATA_SYSTEM_PROMPT = """You are a data catalog assistant helping analysts document tables from government data sources.

Your task is to analyze a table from a parsed document and suggest appropriate metadata including:
1. A clear, descriptive display name
2. A detailed description of what the table contains
3. The data domain (immigration, demographics, economics, etc.)
4. Column-level metadata with semantic types

## Guidelines:

### Display Name
- Use title case
- Be specific about what the table contains
- Include the data source context when helpful
- Example: "USCIS Form I-130 Quarterly Processing Statistics"

### Description
- Explain what data the table contains
- Mention the time period covered if evident
- Note any important context about data collection or meaning
- 2-4 sentences

### Data Domain
Choose from: immigration, demographics, economics, employment, education, healthcare, housing, environment, transportation, public-safety, other

### Column Metadata
For each column, provide:
- display_name: Human-readable name in title case
- description: What the column contains
- data_type: category, numeric, percentage, currency, date, text, identifier
- semantic_type: fiscal_year, calendar_year, quarter, month, country_code, state_code, form_type, approval_rate, denial_rate, processing_count, processing_time, etc.
- unit: If applicable (days, dollars, percent, count)

Be thorough and accurate. The metadata you provide will be used for data discovery and understanding."""

# Tool definition for table metadata extraction
TABLE_METADATA_TOOL = {
    "name": "extract_table_metadata",
    "description": "Extract metadata for a data table",
    "input_schema": {
        "type": "object",
        "required": ["display_name", "description", "data_domain", "columns"],
        "properties": {
            "display_name": {
                "type": "string",
                "description": "Human-readable table name in title case",
            },
            "description": {
                "type": "string",
                "description": "2-4 sentence description of the table contents",
            },
            "data_domain": {
                "type": "string",
                "enum": [
                    "immigration",
                    "demographics",
                    "economics",
                    "employment",
                    "education",
                    "healthcare",
                    "housing",
                    "environment",
                    "transportation",
                    "public-safety",
                    "other",
                ],
                "description": "Primary data domain",
            },
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["column_name", "display_name", "description", "data_type"],
                    "properties": {
                        "column_name": {
                            "type": "string",
                            "description": "Original column name",
                        },
                        "display_name": {
                            "type": "string",
                            "description": "Human-readable name",
                        },
                        "description": {
                            "type": "string",
                            "description": "Description of column contents",
                        },
                        "data_type": {
                            "type": "string",
                            "enum": [
                                "category",
                                "numeric",
                                "percentage",
                                "currency",
                                "date",
                                "text",
                                "identifier",
                            ],
                        },
                        "semantic_type": {
                            "type": "string",
                            "description": "Semantic meaning (fiscal_year, approval_rate, etc.)",
                        },
                        "unit": {
                            "type": "string",
                            "description": "Unit of measurement if applicable",
                        },
                    },
                },
            },
        },
    },
}

# System prompt for column metadata suggestion
COLUMN_METADATA_SYSTEM_PROMPT = """You are a data catalog assistant helping analysts document a specific column from a government data table.

Your task is to analyze a column and suggest appropriate metadata based on:
- The column name
- Sample values from the column
- Context from the document and table

## Guidelines:

### Display Name
- Use title case
- Be specific and descriptive
- Example: "Fiscal Year" not "FY", "Approval Rate" not "Rate"

### Description
- Explain what the column contains in 1-2 sentences
- Mention any important context about the values

### Data Type
Choose from: category, numeric, percentage, currency, date, text, identifier

### Semantic Type
Suggest a semantic type if applicable. Common types:
- Time: fiscal_year, calendar_year, quarter, month, date
- Geography: country_code, state_code, county_code, region
- Immigration: form_type, visa_category, petition_type
- Metrics: approval_rate, denial_rate, processing_count, processing_time, pending_count

### Unit
If the column has a unit of measurement, specify it (days, dollars, percent, count)"""

# Tool definition for column metadata extraction
COLUMN_METADATA_TOOL = {
    "name": "extract_column_metadata",
    "description": "Extract metadata for a table column",
    "input_schema": {
        "type": "object",
        "required": ["display_name", "description", "data_type"],
        "properties": {
            "display_name": {
                "type": "string",
                "description": "Human-readable column name in title case",
            },
            "description": {
                "type": "string",
                "description": "1-2 sentence description of column contents",
            },
            "data_type": {
                "type": "string",
                "enum": [
                    "category",
                    "numeric",
                    "percentage",
                    "currency",
                    "date",
                    "text",
                    "identifier",
                ],
            },
            "semantic_type": {
                "type": "string",
                "description": "Semantic meaning (fiscal_year, approval_rate, etc.)",
            },
            "unit": {
                "type": "string",
                "description": "Unit of measurement if applicable",
            },
        },
    },
}

# System prompt for relationship inference
RELATIONSHIP_SYSTEM_PROMPT = """You are a data catalog assistant helping analysts discover relationships between tables.

Your task is to analyze multiple tables and identify potential relationships based on:
- Common column names or similar columns
- Shared key columns (IDs, codes, categories)
- Logical connections based on the data domains

## Guidelines:

### Relationship Types
- one-to-many: One row in source matches many rows in target
- many-to-one: Many rows in source match one row in target
- many-to-many: Many rows in source can match many rows in target

### Common Patterns
- Fiscal year columns often join across tables
- Form type/category columns create relationships
- Geographic codes (state, country) link data
- Program/visa category columns connect related tables

Be conservative - only suggest relationships where there's clear evidence of a connection."""

# Tool definition for relationship inference
RELATIONSHIP_TOOL = {
    "name": "infer_relationships",
    "description": "Infer relationships between tables",
    "input_schema": {
        "type": "object",
        "required": ["relationships"],
        "properties": {
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "source_table",
                        "target_table",
                        "source_column",
                        "target_column",
                        "relationship_type",
                    ],
                    "properties": {
                        "source_table": {
                            "type": "string",
                            "description": "Source table ID",
                        },
                        "target_table": {
                            "type": "string",
                            "description": "Target table ID",
                        },
                        "source_column": {
                            "type": "string",
                            "description": "Column in source table",
                        },
                        "target_column": {
                            "type": "string",
                            "description": "Column in target table",
                        },
                        "relationship_type": {
                            "type": "string",
                            "enum": ["one-to-many", "many-to-one", "many-to-many"],
                        },
                        "description": {
                            "type": "string",
                            "description": "Description of the relationship",
                        },
                    },
                },
            },
        },
    },
}
