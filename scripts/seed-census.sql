-- ============================================================
-- Census Bureau reference data seed
-- Source: github.com/uscensusbureau/us-census-bureau-data-api-mcp
-- ============================================================

-- Summary levels: geographic hierarchy definitions
INSERT INTO census_summary_levels (code, name, description, get_variable, query_name, on_spine, hierarchy_level, parent_summary_level) VALUES
('010', 'United States', 'The entire United States', 'NAME', 'us', TRUE, 0, NULL),
('020', 'Region', 'Census region (Northeast, Midwest, South, West)', 'NAME', 'region', TRUE, 1, '010'),
('030', 'Division', 'Census division (e.g., New England, Pacific)', 'NAME', 'division', TRUE, 2, '020'),
('040', 'State', 'U.S. state or equivalent (DC, PR, territories)', 'NAME', 'state', TRUE, 3, '010'),
('050', 'County', 'County or equivalent (parish, borough, municipio)', 'NAME', 'county', TRUE, 4, '040'),
('060', 'County Subdivision', 'Minor civil division or census county division', 'NAME', 'county subdivision', FALSE, 5, '050'),
('067', 'Subminor Civil Division', 'Subminor civil division', 'NAME', 'subminor civil division', FALSE, 6, '060'),
('140', 'Census Tract', 'Statistical subdivision of a county (avg ~4,000 people)', 'NAME', 'tract', TRUE, 5, '050'),
('150', 'Block Group', 'Statistical subdivision of a tract (avg ~1,500 people)', 'NAME', 'block group', TRUE, 6, '140'),
('160', 'Place', 'Incorporated place or census designated place', 'NAME', 'place', TRUE, 5, '040'),
('170', 'Consolidated City', 'Consolidated city-county government', 'NAME', 'consolidated city', FALSE, 5, '040'),
('230', 'Alaska Native Regional Corporation', 'Alaska Native regional corporation', 'NAME', 'alaska native regional corporation', FALSE, 5, '040'),
('250', 'American Indian Area/Alaska Native Area/Hawaiian Home Land', 'Tribal and native areas', 'NAME', 'american indian area/alaska native area/hawaiian home land', FALSE, 4, '010'),
('251', 'American Indian Tribal Subdivision', 'Tribal subdivision', 'NAME', 'tribal subdivision', FALSE, 5, '250'),
('252', 'American Indian Reservation (Federal)', 'Federal reservation', 'NAME', 'american indian area (off-reservation trust land only)/hawaiian home land', FALSE, 5, '250'),
('254', 'American Indian Reservation (State)', 'State reservation', 'NAME', 'american indian area', FALSE, 5, '250'),
('256', 'Tribal Census Tract', 'Census tract on tribal land', 'NAME', 'tribal census tract', FALSE, 6, '250'),
('310', 'Metropolitan Statistical Area', 'Metro area (50,000+ population core)', 'NAME', 'metropolitan statistical area/micropolitan statistical area', TRUE, 4, '010'),
('314', 'Metropolitan Division', 'Division within large metro areas', 'NAME', 'metropolitan division', FALSE, 5, '310'),
('320', 'Combined Statistical Area', 'Combination of adjacent metro/micro areas', 'NAME', 'combined statistical area', FALSE, 4, '010'),
('330', 'Combined NECTA', 'New England city and town area combination', 'NAME', 'combined new england city and town area', FALSE, 4, '010'),
('335', 'Combined NECTA Division', 'New England city and town area division', 'NAME', 'new england city and town area', FALSE, 5, '330'),
('350', 'NECTA', 'New England city and town area', 'NAME', 'new england city and town area', FALSE, 4, '010'),
('355', 'NECTA Division', 'Division within large NECTAs', 'NAME', 'necta division', FALSE, 5, '350'),
('400', 'Urban Area', 'Urbanized area or urban cluster', 'NAME', 'urban area', FALSE, 4, '010'),
('500', 'Congressional District', 'U.S. Congressional district', 'NAME', 'congressional district', TRUE, 4, '040'),
('510', 'Congressional District-County', 'Intersection of congressional district and county', 'NAME', 'congressional district', FALSE, 5, '500'),
('610', 'State Legislative District (Upper)', 'State senate district', 'NAME', 'state legislative district (upper chamber)', FALSE, 4, '040'),
('620', 'State Legislative District (Lower)', 'State house/assembly district', 'NAME', 'state legislative district (lower chamber)', FALSE, 4, '040'),
('795', 'Public Use Microdata Area', 'PUMA (100,000+ people, for microdata geography)', 'NAME', 'public use microdata area', FALSE, 4, '040'),
('860', 'ZIP Code Tabulation Area', '5-digit ZCTA (approximation of ZIP code boundaries)', 'NAME', 'zip code tabulation area', TRUE, 4, '010'),
('950', 'School District (Elementary)', 'Elementary school district', 'NAME', 'school district (elementary)', FALSE, 4, '040'),
('960', 'School District (Secondary)', 'Secondary school district', 'NAME', 'school district (secondary)', FALSE, 4, '040'),
('970', 'School District (Unified)', 'Unified school district', 'NAME', 'school district (unified)', FALSE, 4, '040')
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    get_variable = EXCLUDED.get_variable,
    query_name = EXCLUDED.query_name,
    on_spine = EXCLUDED.on_spine,
    hierarchy_level = EXCLUDED.hierarchy_level,
    parent_summary_level = EXCLUDED.parent_summary_level;

-- Seed core geographies: 50 states + DC + PR
INSERT INTO census_geographies (name, full_name, state_code, fips_code, summary_level_code, for_param, in_param, year) VALUES
('Alabama', 'State of Alabama', '01', '01', '040', 'state:01', NULL, 2023),
('Alaska', 'State of Alaska', '02', '02', '040', 'state:02', NULL, 2023),
('Arizona', 'State of Arizona', '04', '04', '040', 'state:04', NULL, 2023),
('Arkansas', 'State of Arkansas', '05', '05', '040', 'state:05', NULL, 2023),
('California', 'State of California', '06', '06', '040', 'state:06', NULL, 2023),
('Colorado', 'State of Colorado', '08', '08', '040', 'state:08', NULL, 2023),
('Connecticut', 'State of Connecticut', '09', '09', '040', 'state:09', NULL, 2023),
('Delaware', 'State of Delaware', '10', '10', '040', 'state:10', NULL, 2023),
('Florida', 'State of Florida', '12', '12', '040', 'state:12', NULL, 2023),
('Georgia', 'State of Georgia', '13', '13', '040', 'state:13', NULL, 2023),
('Hawaii', 'State of Hawaii', '15', '15', '040', 'state:15', NULL, 2023),
('Idaho', 'State of Idaho', '16', '16', '040', 'state:16', NULL, 2023),
('Illinois', 'State of Illinois', '17', '17', '040', 'state:17', NULL, 2023),
('Indiana', 'State of Indiana', '18', '18', '040', 'state:18', NULL, 2023),
('Iowa', 'State of Iowa', '19', '19', '040', 'state:19', NULL, 2023),
('Kansas', 'State of Kansas', '20', '20', '040', 'state:20', NULL, 2023),
('Kentucky', 'Commonwealth of Kentucky', '21', '21', '040', 'state:21', NULL, 2023),
('Louisiana', 'State of Louisiana', '22', '22', '040', 'state:22', NULL, 2023),
('Maine', 'State of Maine', '23', '23', '040', 'state:23', NULL, 2023),
('Maryland', 'State of Maryland', '24', '24', '040', 'state:24', NULL, 2023),
('Massachusetts', 'Commonwealth of Massachusetts', '25', '25', '040', 'state:25', NULL, 2023),
('Michigan', 'State of Michigan', '26', '26', '040', 'state:26', NULL, 2023),
('Minnesota', 'State of Minnesota', '27', '27', '040', 'state:27', NULL, 2023),
('Mississippi', 'State of Mississippi', '28', '28', '040', 'state:28', NULL, 2023),
('Missouri', 'State of Missouri', '29', '29', '040', 'state:29', NULL, 2023),
('Montana', 'State of Montana', '30', '30', '040', 'state:30', NULL, 2023),
('Nebraska', 'State of Nebraska', '31', '31', '040', 'state:31', NULL, 2023),
('Nevada', 'State of Nevada', '32', '32', '040', 'state:32', NULL, 2023),
('New Hampshire', 'State of New Hampshire', '33', '33', '040', 'state:33', NULL, 2023),
('New Jersey', 'State of New Jersey', '34', '34', '040', 'state:34', NULL, 2023),
('New Mexico', 'State of New Mexico', '35', '35', '040', 'state:35', NULL, 2023),
('New York', 'State of New York', '36', '36', '040', 'state:36', NULL, 2023),
('North Carolina', 'State of North Carolina', '37', '37', '040', 'state:37', NULL, 2023),
('North Dakota', 'State of North Dakota', '38', '38', '040', 'state:38', NULL, 2023),
('Ohio', 'State of Ohio', '39', '39', '040', 'state:39', NULL, 2023),
('Oklahoma', 'State of Oklahoma', '40', '40', '040', 'state:40', NULL, 2023),
('Oregon', 'State of Oregon', '41', '41', '040', 'state:41', NULL, 2023),
('Pennsylvania', 'Commonwealth of Pennsylvania', '42', '42', '040', 'state:42', NULL, 2023),
('Rhode Island', 'State of Rhode Island', '44', '44', '040', 'state:44', NULL, 2023),
('South Carolina', 'State of South Carolina', '45', '45', '040', 'state:45', NULL, 2023),
('South Dakota', 'State of South Dakota', '46', '46', '040', 'state:46', NULL, 2023),
('Tennessee', 'State of Tennessee', '47', '47', '040', 'state:47', NULL, 2023),
('Texas', 'State of Texas', '48', '48', '040', 'state:48', NULL, 2023),
('Utah', 'State of Utah', '49', '49', '040', 'state:49', NULL, 2023),
('Vermont', 'State of Vermont', '50', '50', '040', 'state:50', NULL, 2023),
('Virginia', 'Commonwealth of Virginia', '51', '51', '040', 'state:51', NULL, 2023),
('Washington', 'State of Washington', '53', '53', '040', 'state:53', NULL, 2023),
('West Virginia', 'State of West Virginia', '54', '54', '040', 'state:54', NULL, 2023),
('Wisconsin', 'State of Wisconsin', '55', '55', '040', 'state:55', NULL, 2023),
('Wyoming', 'State of Wyoming', '56', '56', '040', 'state:56', NULL, 2023),
('District of Columbia', 'District of Columbia', '11', '11', '040', 'state:11', NULL, 2023),
('Puerto Rico', 'Commonwealth of Puerto Rico', '72', '72', '040', 'state:72', NULL, 2023)
ON CONFLICT (fips_code, year) DO UPDATE SET
    name = EXCLUDED.name,
    full_name = EXCLUDED.full_name,
    state_code = EXCLUDED.state_code,
    summary_level_code = EXCLUDED.summary_level_code,
    for_param = EXCLUDED.for_param;

-- Seed US-level geography
INSERT INTO census_geographies (name, full_name, fips_code, summary_level_code, for_param, in_param, year)
VALUES ('United States', 'United States', '00', '010', 'us:*', NULL, 2023)
ON CONFLICT (fips_code, year) DO NOTHING;

-- Seed Census regions
INSERT INTO census_geographies (name, full_name, fips_code, summary_level_code, for_param, in_param, region_code, year) VALUES
('Northeast', 'Northeast Region', 'R1', '020', 'region:1', NULL, '1', 2023),
('Midwest', 'Midwest Region', 'R2', '020', 'region:2', NULL, '2', 2023),
('South', 'South Region', 'R3', '020', 'region:3', NULL, '3', 2023),
('West', 'West Region', 'R4', '020', 'region:4', NULL, '4', 2023)
ON CONFLICT (fips_code, year) DO NOTHING;

-- Seed core programs
INSERT INTO census_programs (label, description, acronym) VALUES
('American Community Survey', 'Annual survey of social, economic, housing, and demographic characteristics', 'ACS'),
('Decennial Census', 'Complete count of the U.S. population conducted every 10 years', 'DEC'),
('Population Estimates', 'Annual estimates of population and components of change', 'PEP'),
('Economic Census', 'Comprehensive statistics about U.S. businesses every 5 years', 'ECN'),
('Annual Business Survey', 'Annual survey of business characteristics', 'ABS'),
('County Business Patterns', 'Annual data on business establishments, employment, and payroll', 'CBP')
ON CONFLICT (acronym) DO NOTHING;

-- Seed core components (most-used ACS endpoints)
INSERT INTO census_components (label, component_id, api_endpoint, description, program_id) VALUES
('ACS 1-Year Detailed Tables', 'ACSDT1Y', 'acs/acs1', 'Detailed tables from the ACS 1-year estimates', (SELECT id FROM census_programs WHERE acronym = 'ACS')),
('ACS 5-Year Detailed Tables', 'ACSDT5Y', 'acs/acs5', 'Detailed tables from the ACS 5-year estimates', (SELECT id FROM census_programs WHERE acronym = 'ACS')),
('ACS 1-Year Subject Tables', 'ACSST1Y', 'acs/acs1/subject', 'Subject tables from the ACS 1-year estimates', (SELECT id FROM census_programs WHERE acronym = 'ACS')),
('ACS 5-Year Subject Tables', 'ACSST5Y', 'acs/acs5/subject', 'Subject tables from the ACS 5-year estimates', (SELECT id FROM census_programs WHERE acronym = 'ACS')),
('ACS 1-Year Data Profiles', 'ACSDP1Y', 'acs/acs1/profile', 'Data profiles from the ACS 1-year estimates', (SELECT id FROM census_programs WHERE acronym = 'ACS')),
('ACS 5-Year Data Profiles', 'ACSDP5Y', 'acs/acs5/profile', 'Data profiles from the ACS 5-year estimates', (SELECT id FROM census_programs WHERE acronym = 'ACS')),
('Decennial Census PL 94-171', 'DECPL', 'dec/pl', 'Redistricting data from the decennial census', (SELECT id FROM census_programs WHERE acronym = 'DEC')),
('Decennial Census DHC', 'DECDHC', 'dec/dhc', 'Demographic and Housing Characteristics', (SELECT id FROM census_programs WHERE acronym = 'DEC')),
('Population Estimates', 'PEP', 'pep/population', 'Annual population estimates', (SELECT id FROM census_programs WHERE acronym = 'PEP')),
('County Business Patterns', 'CBP', 'cbp', 'Annual county-level business statistics', (SELECT id FROM census_programs WHERE acronym = 'CBP'))
ON CONFLICT (component_id) DO NOTHING;
