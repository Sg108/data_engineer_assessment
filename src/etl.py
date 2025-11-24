import json
import re
import mysql.connector

# -------------- CONFIG --------------
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "db_user"
MYSQL_PASSWORD = "6equj5_db_user"
MYSQL_DB = "home_db"

#----Change the location to where ever you have the data present----
JSON_PATH = "../data/property_data.json"  


def to_int(value):
    """Convert messy numeric-like values to int (e.g. '5649 sqft', 'Five')."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return int(value)

    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in ("null", "na"):
            return None

        # word numbers for Bed etc.
        word_map = {
            "one": 1, "two": 2, "three": 3, "four": 4,
            "five": 5, "six": 6, "seven": 7, "eight": 8,
        }
        low = s.lower()
        if low in word_map:
            return word_map[low]

        # extract digits (handles '9191 sqfts')
        digits = re.findall(r"\d+", s)
        if digits:
            return int("".join(digits))

    return None


def to_float(value):
    """Convert to float if possible."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in ("null", "na"):
            return None
        # keep digits, dot, minus
        s_clean = re.sub(r"[^0-9.\-]", "", s)
        if not s_clean:
            return None
        try:
            return float(s_clean)
        except ValueError:
            return None
    return None


def clean_str(value):
    """Strip, turn empty/'Null' -> None."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() == "null":
            return None
        return s
    return str(value)


def main():
    # 1. Read JSON
    with open(JSON_PATH, "r") as f:
        records = json.load(f)

    # 2. Connect to MySQL
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Prepare insert statements
    insert_property_sql = """
        INSERT INTO properties (
          property_title, address, market, flood, street_address, city, state, zip,
          property_type, highway, train, tax_rate, sqft_basement, htw, pool,
          commercial, water, sewage, year_built, sqft_mu, sqft_total,
          parking, bed, bath, basement_yes_no, layout,
           rent_restricted, neighborhood_rating,
          latitude, longitude, subdivision, school_average
        )
        VALUES (
          %(property_title)s, %(address)s, %(market)s, %(flood)s, %(street_address)s,
          %(city)s, %(state)s, %(zip)s, %(property_type)s, %(highway)s, %(train)s,
          %(tax_rate)s, %(sqft_basement)s, %(htw)s, %(pool)s, %(commercial)s,
          %(water)s, %(sewage)s, %(year_built)s, %(sqft_mu)s, %(sqft_total)s,
          %(parking)s, %(bed)s, %(bath)s, %(basement_yes_no)s, %(layout)s,
           %(rent_restricted)s, %(neighborhood_rating)s,
          %(latitude)s, %(longitude)s, %(subdivision)s, 
          %(school_average)s
        )
    """

    insert_valuation_sql = """
        INSERT INTO valuations (
         property_id, scenario_index, list_price, previous_rent, arv,
          expected_rent, rent_zestimate, low_fmr, high_fmr,
          zestimate, redfin_value
        )
        VALUES (
          %(property_id)s, %(scenario_index)s, %(list_price)s, %(previous_rent)s, %(arv)s,
          %(expected_rent)s, %(rent_zestimate)s, %(low_fmr)s, %(high_fmr)s,
          %(zestimate)s, %(redfin_value)s
        )
    """

    insert_hoa_sql = """
        INSERT INTO hoa (
        property_id, hoa_amount, hoa_flag
        )
        VALUES (
          %(property_id)s, %(hoa_amount)s, %(hoa_flag)s
        )
    """

    insert_rehab_sql = """
        INSERT INTO rehab (
          property_id, underwriting_rehab, rehab_calculation,
          paint, flooring_flag, foundation_flag, roof_flag, hvac_flag,
          kitchen_flag, bathroom_flag, appliances_flag, windows_flag,
          landscaping_flag, trashout_flag
        )
        VALUES (
          %(property_id)s, %(underwriting_rehab)s, %(rehab_calculation)s,
          %(paint)s, %(flooring_flag)s, %(foundation_flag)s, %(roof_flag)s, %(hvac_flag)s,
          %(kitchen_flag)s, %(bathroom_flag)s, %(appliances_flag)s, %(windows_flag)s,
          %(landscaping_flag)s, %(trashout_flag)s
        )
    """
    insert_tax_sql= """  
        INSERT INTO taxes (
          property_id, taxes )
     VALUES (
          %(property_id)s, %(taxes)s )

    """
    
    insert_leads_sql = """
          INSERT INTO leads (
          property_id, reviewed_status, most_recent_status,
          source,occupancy, net_yield, irr,selling_reason,
          seller_retained_broker, final_reviewer 
        )
        VALUES (
          %(property_id)s, %(reviewed_status)s, %(most_recent_status)s,
          %(source)s, %(occupancy)s, %(net_yield)s, %(irr)s, %(selling_reason)s, %(seller_retained_broker)s,
          %(final_reviewer)s 
          ) 
    """
     
    try:
        for rec in records:
            # ---------- PROPERTIES ----------
            prop_data = {
                "property_title":      clean_str(rec.get("Property_Title")),
                "address":             clean_str(rec.get("Address")),
                "market":              clean_str(rec.get("Market")),
                "flood":               clean_str(rec.get("Flood")),
                "street_address":      clean_str(rec.get("Street_Address")),
                "city":                clean_str(rec.get("City")),
                "state":               clean_str(rec.get("State")),
                "zip":                 clean_str(rec.get("Zip")),
                "property_type":       clean_str(rec.get("Property_Type")),
                "highway":             clean_str(rec.get("Highway")),
                "train":               clean_str(rec.get("Train")),
                "tax_rate":            to_float(rec.get("Tax_Rate")),
                "sqft_basement":       to_int(rec.get("SQFT_Basement")),
                "htw":                 clean_str(rec.get("HTW")),
                "pool":                clean_str(rec.get("Pool")),
                "commercial":          clean_str(rec.get("Commercial")),
                "water":               clean_str(rec.get("Water")),
                "sewage":              clean_str(rec.get("Sewage")),
                "year_built":          to_int(rec.get("Year_Built")),
                "sqft_mu":             to_int(rec.get("SQFT_MU")),
                "sqft_total":          to_int(rec.get("SQFT_Total")),
                "parking":             clean_str(rec.get("Parking")),
                "bed":                 to_int(rec.get("Bed")),
                "bath":                to_int(rec.get("Bath")),
                "basement_yes_no":     clean_str(rec.get("BasementYesNo")),
                "layout":              clean_str(rec.get("Layout")),
                "rent_restricted":     clean_str(rec.get("Rent_Restricted")),
                "neighborhood_rating": to_int(rec.get("Neighborhood_Rating")),
                "latitude":            to_float(rec.get("Latitude")),
                "longitude":           to_float(rec.get("Longitude")),
                "subdivision":         clean_str(rec.get("Subdivision")),
                "school_average":      to_float(rec.get("School_Average")),
            }

            cur.execute(insert_property_sql, prop_data)
            property_id = cur.lastrowid
            
 	    # ---------- LEADS INSERT ----------

            l_row = {   "property_id":   property_id,
                        "reviewed_status":     clean_str(rec.get("Reviewed_Status")),
                	"most_recent_status":  clean_str(rec.get("Most_Recent_Status")),
                	"source":              clean_str(rec.get("Source")),
     			"occupancy":           clean_str(rec.get("Occupancy")),
			"net_yield":           to_float(rec.get("Net_Yield")),
                	"irr":                 to_float(rec.get("IRR")),
              		"selling_reason":      clean_str(rec.get("Selling_Reason")),
                	"seller_retained_broker": clean_str(rec.get("Seller_Retained_Broker")),
                	"final_reviewer":      clean_str(rec.get("Final_Reviewer"))
		    }
            
            cur.execute(insert_leads_sql, l_row)

 	    # ---------- TAXES INSERT ----------

            t_row={   "property_id":   property_id,
                      "taxes":      to_int(rec.get("Taxes")) }

            cur.execute(insert_tax_sql, t_row)

            # ---------- VALUATIONS INSERT ----------
            valuations = rec.get("Valuation") or []
            for idx, v in enumerate(valuations):
                v_row = {
                    "property_id":   property_id,
                    "scenario_index": idx,
                    "list_price":    to_int(v.get("List_Price")),
                    "previous_rent": to_int(v.get("Previous_Rent")),
                    "arv":           to_int(v.get("ARV")),
                    "expected_rent": to_int(v.get("Expected_Rent")),
                    "rent_zestimate": to_int(v.get("Rent_Zestimate")),
                    "low_fmr":       to_int(v.get("Low_FMR")),
                    "high_fmr":      to_int(v.get("High_FMR")),
                    "zestimate":     to_int(v.get("Zestimate")),
                    "redfin_value":  to_int(v.get("Redfin_Value")),
                }
                cur.execute(insert_valuation_sql, v_row)

            # ---------- HOA INSERT ----------
            hoas = rec.get("HOA") or []
            for h in hoas:
                h_row = {
                    "property_id": property_id,
                    "hoa_amount":  to_int(h.get("HOA")),
                    "hoa_flag":    clean_str(h.get("HOA_Flag")),
                }
                cur.execute(insert_hoa_sql, h_row)

            # ---------- REHAB INSERT ----------
            rehabs = rec.get("Rehab") or []
            for r in rehabs:
                r_row = {
                    "property_id":        property_id,
                    "underwriting_rehab": to_int(r.get("Underwriting_Rehab")),
                    "rehab_calculation":  to_int(r.get("Rehab_Calculation")),
                    "paint":              clean_str(r.get("Paint")),
                    "flooring_flag":      clean_str(r.get("Flooring_Flag")),
                    "foundation_flag":    clean_str(r.get("Foundation_Flag")),
                    "roof_flag":          clean_str(r.get("Roof_Flag")),
                    "hvac_flag":          clean_str(r.get("HVAC_Flag")),
                    "kitchen_flag":       clean_str(r.get("Kitchen_Flag")),
                    "bathroom_flag":      clean_str(r.get("Bathroom_Flag")),
                    "appliances_flag":    clean_str(r.get("Appliances_Flag")),
                    "windows_flag":       clean_str(r.get("Windows_Flag")),
                    "landscaping_flag":   clean_str(r.get("Landscaping_Flag")),
                    "trashout_flag":      clean_str(r.get("Trashout_Flag")),
                }
                cur.execute(insert_rehab_sql, r_row)

        conn.commit()
        print("ETL complete: data loaded into properties and all the related tables.")

    except Exception as e:
        conn.rollback()
        print("Error during ETL, rolled back:", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()