
USE home_db;

-- Drop existing tables if you want a clean slate
DROP TABLE IF EXISTS rehab;
DROP TABLE IF EXISTS hoa;
DROP TABLE IF EXISTS valuations;
DROP TABLE IF EXISTS taxes;
DROP TABLE IF EXISTS leads;
DROP TABLE IF EXISTS properties;


-- Parent table: properties
CREATE TABLE properties (
  property_id             INT AUTO_INCREMENT PRIMARY KEY,
  property_title          VARCHAR(255),
  address                 VARCHAR(255),
  market                  VARCHAR(50),
  flood                   VARCHAR(50),
  street_address          VARCHAR(255),
  city                    VARCHAR(100),
  state                   VARCHAR(10),
  zip                     VARCHAR(10),
  property_type           VARCHAR(50),
  highway                 VARCHAR(20),
  train                   VARCHAR(20),
  tax_rate                DECIMAL(8,4),
  sqft_basement           INT,
  htw                     VARCHAR(10),
  pool                    VARCHAR(10),
  commercial              VARCHAR(10),
  water                   VARCHAR(20),
  sewage                  VARCHAR(20),
  year_built              INT,
  sqft_mu                 INT,
  sqft_total              INT,
  parking                 VARCHAR(50),
  bed                     INT,
  bath                    INT,
  basement_yes_no         VARCHAR(10),
  layout                  VARCHAR(50),
  rent_restricted         VARCHAR(10),
  neighborhood_rating     INT,
  latitude                DECIMAL(10,6),
  longitude               DECIMAL(10,6),
  subdivision             VARCHAR(100),
  school_average          DECIMAL(8,4)
);

-- Leads
CREATE TABLE leads (
  leads_id    		 INT AUTO_INCREMENT PRIMARY KEY,
  property_id 		 INT NOT NULL,
  Reviewed_status        VARCHAR(50),
  Most_recent_status     VARCHAR(50),
  Source                 VARCHAR(50),
  Occupancy       	 VARCHAR(20),
  Net_Yield       	 DECIMAL(8,4),
  IRR               	 DECIMAL(8,4),
  Selling_Reason         VARCHAR(100),
  Seller_Retained_Broker VARCHAR(10),
  Final_Reviewer         VARCHAR(100),
  CONSTRAINT fk_leads_property
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
    ON DELETE CASCADE
);

-- Valuations: 1-to-many with properties
CREATE TABLE valuations (
  valuation_id    INT AUTO_INCREMENT PRIMARY KEY,
  property_id     INT NOT NULL,
  scenario_index  INT,
  list_price      INT,
  previous_rent   INT,
  arv             INT,
  expected_rent   INT,
  rent_zestimate  INT,
  low_fmr         INT,
  high_fmr        INT,
  zestimate       INT,
  redfin_value    INT,
  CONSTRAINT fk_valuations_property
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
    ON DELETE CASCADE
);

-- HOA 
CREATE TABLE hoa (
  hoa_id      INT AUTO_INCREMENT PRIMARY KEY,
  property_id INT NOT NULL,
  hoa_amount  INT,
  hoa_flag    VARCHAR(5),
  CONSTRAINT fk_hoa_property
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
    ON DELETE CASCADE
);

-- Rehab 
CREATE TABLE rehab (
  rehab_id            INT AUTO_INCREMENT PRIMARY KEY,
  property_id         INT NOT NULL,
  underwriting_rehab  INT,
  rehab_calculation   INT,
  paint               VARCHAR(5),
  flooring_flag       VARCHAR(5),
  foundation_flag     VARCHAR(5),
  roof_flag           VARCHAR(5),
  hvac_flag           VARCHAR(5),
  kitchen_flag        VARCHAR(5),
  bathroom_flag       VARCHAR(5),
  appliances_flag     VARCHAR(5),
  windows_flag        VARCHAR(5),
  landscaping_flag    VARCHAR(5),
  trashout_flag       VARCHAR(5),
  CONSTRAINT fk_rehab_property
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
    ON DELETE CASCADE
);

-- Taxes
CREATE TABLE taxes (
  tax_id              INT AUTO_INCREMENT PRIMARY KEY,
  property_id         INT NOT NULL,
  taxes  INT,
  CONSTRAINT fk_tax_property
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
    ON DELETE CASCADE
);