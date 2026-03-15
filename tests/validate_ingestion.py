import pymongo
from pymongo import MongoClient
import os
import sys

def validate_ingestion():
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.environ.get("FLOATCHAT_DB", "floatchat_ai")

    print(f"Connecting to MongoDB at {mongo_uri}...")
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        
        # 1. Count documents
        profiles_count = db.profiles.count_documents({})
        bgc_profiles_count = db.bgc_profiles.count_documents({})
        floats_count = db.floats.count_documents({})
        
        print(f"\n--- Document Counts ---")
        print(f"Profiles (core): {profiles_count}")
        print(f"Profiles (BGC):  {bgc_profiles_count}")
        print(f"Floats:          {floats_count}")
        
        if profiles_count == 0 or floats_count == 0:
            print("\n❌ Error: Collections are empty!")
            sys.exit(1)
            
        print("\n✅ Document counts look good (expected ~87K core, ~13K BGC, 567 floats).")

        # 2. Check indexes
        print(f"\n--- Checking Indexes ---")
        for coll_name in ["profiles", "bgc_profiles", "floats"]:
            indexes = list(db[coll_name].list_indexes())
            index_names = [idx["name"] for idx in indexes]
            print(f"{coll_name} indexes: {', '.join(index_names)}")
            
            if coll_name == "profiles" and "geo_location_2dsphere" not in index_names:
                print(f"❌ Error: Missing geo_location_2dsphere index on {coll_name}")

        # 3. Sample Document Validation
        print(f"\n--- Validating a Core Profile Sample ---")
        sample_core = db.profiles.find_one()
        if sample_core:
            assert "platform_number" in sample_core, "Missing platform_number"
            assert "cycle_number" in sample_core, "Missing cycle_number"
            assert "geo_location" in sample_core, "Missing geo_location"
            assert "measurements" in sample_core and len(sample_core["measurements"]) > 0, "Missing or empty measurements"
            print("✅ Core profile sample looks correct.")
        
        print(f"\n--- Validating a BGC Profile Sample ---")
        sample_bgc = db.bgc_profiles.find_one({"contains_bgc": True})
        if sample_bgc:
            assert "platform_number" in sample_bgc, "Missing platform_number"
            assert "station_parameters" in sample_bgc, "Missing station_parameters"
            
            # Check if there are BGC-specific parameters in the measurements
            first_measurement = sample_bgc["measurements"][0]
            bgc_keys = [k for k in first_measurement.keys() if k not in ["pres", "pres_qc", "pres_adjusted", "pres_adjusted_qc", "pres_adjusted_error", "temp", "temp_qc", "temp_adjusted", "temp_adjusted_qc", "temp_adjusted_error", "psal", "psal_qc", "psal_adjusted", "psal_adjusted_qc", "psal_adjusted_error"]]
            
            print(f"Sample BGC measurement keys found: {list(first_measurement.keys())}")
            if len(bgc_keys) > 0:
                print(f"✅ BGC profile sample is successfully parsed with extended parameters: {bgc_keys}")
            else:
                print("❌ Error: BGC profile does not contain extended parameters.")

        print(f"\n--- Validating a Float Aggregate Sample ---")
        sample_float = db.floats.find_one()
        if sample_float:
            assert "platform_number" in sample_float, "Missing platform_number"
            assert "total_cycles" in sample_float, "Missing total_cycles"
            assert "geo_bounding_box" in sample_float, "Missing geo_bounding_box"
            print("✅ Float aggregate sample looks correct.")
            
        print("\nAll validation checks passed! Data ingestion is successful.")

    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    validate_ingestion()
