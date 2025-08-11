"""
Post-LLM Data Lake Generation for SAP ARIBA Ordering System

This script creates additional data structures to mimic a real SAP ARIBA data lake:
1. Cost centres CSV with hierarchy/org info
2. Vendor master CSV with known vendors + aliases
3. Exchange rates CSV for multi-currency support
4. Split line items into country/cost centre files with dates

Author: Generated for Orderly project
Date: August 2025
"""

import json
import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from collections import defaultdict

# Configuration
DATA_DIR = Path(__file__).parent
RAW_DIR = DATA_DIR.parent / "data" / "source"
HIDDEN_DIR = RAW_DIR / "hidden"
SIMULATED_DIR = DATA_DIR / "output"

# Ensure directories exist
RAW_DIR.mkdir(exist_ok=True)
HIDDEN_DIR.mkdir(exist_ok=True)
SIMULATED_DIR.mkdir(exist_ok=True)

# Date range: July 2025 - August 2025
START_DATE = datetime(2025, 7, 1)
END_DATE = datetime(2025, 8, 31)

# Countries and cost centres
COUNTRIES = {
    "AU": {
        "name": "Australia", 
        "currency": "AUD",
        "cost_centres": ["CC_AU_001", "CC_AU_002", "CC_AU_003", "CC_AU_004", "CC_AU_005"]
    },
    "US": {
        "name": "United States", 
        "currency": "USD",
        "cost_centres": ["CC_US_001", "CC_US_002", "CC_US_003", "CC_US_004", "CC_US_005"]
    }
}

# BRANDS
BRANDS = ["TechFlow", "ViewMaster", "ConnectPro", "DataVault", "OfficeLink", "ProConnect"]

def generate_cost_centres_csv():
    """Generate cost centre hierarchy CSV file."""
    print("📊 Generating cost centres CSV...")
    
    cost_centres = []
    
    for country_code, country_info in COUNTRIES.items():
        # Country level
        cost_centres.append({
            "cost_centre_id": f"CC_{country_code}_000",
            "cost_centre_name": f"{country_info['name']} - Corporate",
            "parent_cost_centre_id": None,
            "country_code": country_code,
            "country_name": country_info["name"],
            "currency": country_info["currency"],
            "level": 1,
            "is_active": True,
            "department": "Corporate",
            "budget_limit": 1000000,
            "created_date": "2025-01-01",
            "updated_date": "2025-07-01"
        })
        
        # Department level cost centres
        departments = ["IT", "HR", "Finance", "Marketing", "Operations"]
        for i, cc_id in enumerate(country_info["cost_centres"]):
            cost_centres.append({
                "cost_centre_id": cc_id,
                "cost_centre_name": f"{country_info['name']} - {departments[i]}",
                "parent_cost_centre_id": f"CC_{country_code}_000",
                "country_code": country_code,
                "country_name": country_info["name"],
                "currency": country_info["currency"],
                "level": 2,
                "is_active": True,
                "department": departments[i],
                "budget_limit": random.randint(50000, 200000),
                "created_date": "2025-01-01",
                "updated_date": "2025-07-01"
            })
    
    # Write to CSV
    output_file = RAW_DIR / "cost_centres.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            "cost_centre_id", "cost_centre_name", "parent_cost_centre_id",
            "country_code", "country_name", "currency", "level", "is_active",
            "department", "budget_limit", "created_date", "updated_date"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cost_centres)
    
    print(f"✅ Created {output_file} with {len(cost_centres)} cost centres")

def generate_canonical_brand_table():
    """Generate canonical brand table using global BRANDS as canonical source."""
    print("🏷️  Generating canonical brand table...")
    
    try:
        from rapidfuzz import fuzz, process
    except ImportError:
        print("❌ rapidfuzz not installed. Installing...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "rapidfuzz"])
        from rapidfuzz import fuzz, process
    
    # Read line items and extract all brand variations
    line_item_brands = set()
    try:
        with open(SIMULATED_DIR / "line_items.jsonl", 'r') as f:
            for line in f:
                item = json.loads(line.strip())
                # Extract brands from all entries (not just clean ones)
                if item.get("brand"):
                    line_item_brands.add(item["brand"])
    except FileNotFoundError:
        print("⚠️  line_items.jsonl not found, using simulated brand variations")
        # Add some simulated variations for testing
        line_item_brands = {
            "TechFlow", "techflow", "TF", "Tech Flow",
            "ViewMaster", "Viewmaster", "VM", "View Master", 
            "ConnectPro", "Connect Pro", "CP", "connectpro",
            "DataVault", "Data Vault", "DV", "DatVault", "DatVlt",
            "OfficeLink", "Office Link", "OL", "officelink",
            "ProConnect", "Pro Connect", "PC", "proconnect"
        }
    
    # Special abbreviation mappings (explicit edge cases)
    abbreviation_map = {
        "TF": "TechFlow",
        "VM": "ViewMaster", 
        "CP": "ConnectPro",
        "DV": "DataVault",
        "OL": "OfficeLink",
        "PC": "ProConnect"
    }
    
    # Create canonical brand records with aliases
    canonical_brands = []
    brand_id_counter = 1
    
    for canonical_brand in sorted(BRANDS):
        # Find all variations that match this canonical brand
        brand_aliases = []
        
        for line_brand in line_item_brands:
            # Check exact abbreviation matches first
            if line_brand in abbreviation_map and abbreviation_map[line_brand] == canonical_brand:
                brand_aliases.append(line_brand)
                continue
            
            # Skip if it's the canonical brand itself
            if line_brand == canonical_brand:
                continue
                
            # Use rapidfuzz to find close matches (threshold 75%)
            similarity = fuzz.ratio(line_brand.lower(), canonical_brand.lower())
            if similarity >= 75:
                brand_aliases.append(line_brand)
                print(f"   📎 Matched '{line_brand}' to '{canonical_brand}' (similarity: {similarity}%)")
        
        # Remove duplicates and sort aliases
        brand_aliases = sorted(list(set(brand_aliases)))
        
        canonical_brands.append({
            "brand_id": f"BRD{brand_id_counter:03d}",
            "canonical_brand_name": canonical_brand,
            "brand_aliases": brand_aliases,  # Now populated with variations
            "is_active": True,
            "created_date": "2025-01-01",
            "updated_date": "2025-07-01"
        })
        brand_id_counter += 1
    
    # Write to CSV in hidden directory
    output_file = HIDDEN_DIR / "canonical_brands.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            "brand_id", "canonical_brand_name", "brand_aliases", 
            "is_active", "created_date", "updated_date"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(canonical_brands)
    
    print(f"✅ Created {output_file} with {len(canonical_brands)} canonical brands")
    print(f"   🔗 Total aliases found: {sum(len(brand['brand_aliases']) for brand in canonical_brands)}")
    return BRANDS

def generate_canonical_sku_catalog():
    """Generate canonical SKU catalog from clean line items."""
    print("📦 Generating canonical SKU catalog...")
    
    # Read line items and extract clean SKU information - now stores all instances
    sku_data = {}  # sku_id -> list of instances
    duplicate_warnings = []
    
    try:
        with open(SIMULATED_DIR / "line_items.jsonl", 'r') as f:
            for line in f:
                item = json.loads(line.strip())
                # Only use clean entries for canonical data
                if item.get("data_quality_flag") == "clean" and item.get("sku_id"):
                    sku_id = item["sku_id"]
                    
                    current_instance = {
                        "sku_id": sku_id,
                        "canonical_description": item.get("item_description", ""),
                        "canonical_brand": item.get("brand", ""),
                        "canonical_unit": item.get("unit", "each"),
                        "canonical_size": item.get("size", ""),
                        "category": "",  # Will be inferred from description
                        "is_active": True,
                        "created_date": "2025-01-01",
                        "updated_date": "2025-07-01"
                    }
                    
                    if sku_id not in sku_data:
                        # First instance of this SKU
                        sku_data[sku_id] = [current_instance]
                    else:
                        # Duplicate SKU - check for data consistency
                        first_instance = sku_data[sku_id][0]
                        differences = []
                        
                        # Compare key fields for inconsistencies
                        fields_to_compare = ["canonical_description", "canonical_brand", "canonical_unit", "canonical_size"]
                        for field in fields_to_compare:
                            if first_instance[field] != current_instance[field]:
                                differences.append(f"{field}: '{first_instance[field]}' vs '{current_instance[field]}'")
                        
                        if differences:
                            warning_msg = f"⚠️  SKU {sku_id} has conflicting data: {'; '.join(differences)}"
                            duplicate_warnings.append(warning_msg)
                            print(f"   {warning_msg}")
                        
                        # Append regardless of differences (keep all instances)
                        sku_data[sku_id].append(current_instance)
                        
    except FileNotFoundError:
        print("❌ line_items.jsonl not found")
        return
    
    # Infer categories from descriptions
    category_keywords = {
        "Keyboards": ["keyboard", "kb"],
        "Monitors": ["monitor", "display", "screen"],
        "Webcams": ["webcam", "camera", "cam"],
        "Headsets": ["headset", "headphone", "earphone", "earbuds"],
        "Cables": ["cable", "cord", "wire"],
        "Adapters": ["adapter", "adaptor", "converter"],
        "Storage": ["drive", "storage", "tb", "gb"]
    }
    
    # Map brands to canonical BRANDS using fuzzy matching
    from rapidfuzz import fuzz, process
    
    # Special abbreviation mappings (same as in brand table)
    abbreviation_map = {
        "TF": "TechFlow",
        "VM": "ViewMaster", 
        "CP": "ConnectPro",
        "DV": "DataVault",
        "OL": "OfficeLink",
        "PC": "ProConnect"
    }
    
    def map_to_canonical_brand(brand_name):
        """Map a brand name to canonical BRANDS using fuzzy matching."""
        if not brand_name:
            return ""
        if brand_name in abbreviation_map:
            return abbreviation_map[brand_name]
        if brand_name in BRANDS:
            return brand_name
        best_match, score, _ = process.extractOne(brand_name, BRANDS, scorer=fuzz.ratio)
        if score >= 75:
            return best_match
        return brand_name
    
    # Flatten to all SKU instances - handle duplicates with versioning
    all_sku_instances = []
    total_instances = 0
    duplicate_count = 0
    
    for sku_id, instances in sku_data.items():
        total_instances += len(instances)
        if len(instances) > 1:
            duplicate_count += len(instances) - 1
        
        # Sort instances by some criteria (could use timestamp if available)
        # For now, use order of appearance (first = oldest)
        
        for i, instance in enumerate(instances):
            # Map brand to canonical BRANDS
            instance["canonical_brand"] = map_to_canonical_brand(instance["canonical_brand"])
            
            # Handle duplicate versioning
            if len(instances) > 1:
                if i < len(instances) - 1:
                    # Previous versions - mark as inactive
                    instance["is_active"] = False
                    # Set updated_date to when it was superseded (simulate timeline)
                    days_offset = (i + 1) * 30  # 30 days between versions
                    base_date = datetime(2025, 1, 1)
                    superseded_date = base_date + timedelta(days=days_offset)
                    instance["updated_date"] = superseded_date.strftime("%Y-%m-%d")
                else:
                    # Latest version - keep active
                    instance["is_active"] = True
                    # Set created_date based on version number
                    days_offset = i * 30  # 30 days between versions
                    base_date = datetime(2025, 1, 1)
                    created_date = base_date + timedelta(days=days_offset)
                    instance["created_date"] = created_date.strftime("%Y-%m-%d")
                    instance["updated_date"] = "2025-07-01"  # Latest update
            else:
                # Single instance - keep as active with default dates
                instance["is_active"] = True
                instance["created_date"] = "2025-01-01"
                instance["updated_date"] = "2025-07-01"
            
            # Infer category from description for each instance
            description_lower = instance["canonical_description"].lower()
            category_found = False
            
            for category, keywords in category_keywords.items():
                if any(keyword in description_lower for keyword in keywords):
                    instance["category"] = category
                    category_found = True
                    break
            
            if not category_found:
                instance["category"] = "Other"
            
            all_sku_instances.append(instance)
    
    # Sort by sku_id before saving
    all_sku_instances.sort(key=lambda x: x["sku_id"])
    
    # Print summary of duplicates
    if duplicate_count > 0:
        print(f"   📊 Found {len(duplicate_warnings)} SKUs with conflicting data")
        print(f"   📈 Total instances: {total_instances}, Unique SKUs: {len(sku_data)}, Duplicate instances: {duplicate_count}")
    
    # Write ALL instances to CSV in hidden directory (including duplicates)
    output_file = HIDDEN_DIR / "canonical_sku_catalog.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            "sku_id", "canonical_description", "canonical_brand", 
            "canonical_unit", "canonical_size", "category",
            "is_active", "created_date", "updated_date"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_sku_instances)
    
    print(f"✅ Created {output_file} with {len(all_sku_instances)} SKU instances (including duplicates)")
    return sku_data

def generate_vendor_master_csv():
    """Generate vendor master CSV - one entry per vendor (SAP standard)."""
    print("🏭 Generating vendor master CSV...")
    
    # Read canonical brands instead of product SKUs
    brands = set()
    try:
        canonical_brands_file = HIDDEN_DIR / "canonical_brands.csv"
        if canonical_brands_file.exists():
            with open(canonical_brands_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    brands.add(row["canonical_brand_name"])
        else:
            # Fallback to reading from line items
            with open(SIMULATED_DIR / "line_items.jsonl", 'r') as f:
                for line in f:
                    item = json.loads(line.strip())
                    if item.get("data_quality_flag") == "clean" and item.get("brand"):
                        brands.add(item["brand"])
    except FileNotFoundError:
        print("⚠️  canonical brands or line_items.jsonl not found, using default brands")
        brands = {"TechFlow", "ViewMaster", "ConnectPro", "DataVault", "OfficeLink", "ProConnect"}
    
    vendors = []
    vendor_id_counter = 1000
    
    # Predefined vendor details for realistic SAP vendor master
    vendor_details = {
        "TechFlow": {"country": "US", "city": "San Francisco", "currency": "USD", "terms": "NET30"},
        "ViewMaster": {"country": "AU", "city": "Sydney", "currency": "AUD", "terms": "IMMEDIATE"},
        "ConnectPro": {"country": "DE", "city": "Berlin", "currency": "EUR", "terms": "NET45"},
        "DataVault": {"country": "SG", "city": "Singapore", "currency": "SGD", "terms": "IMMEDIATE"},
        "OfficeLink": {"country": "US", "city": "New York", "currency": "USD", "terms": "NET60"},
        "ProConnect": {"country": "UK", "city": "London", "currency": "GBP", "terms": "NET30"}
    }
    
    for brand in brands:
        vendor_id = f"V{vendor_id_counter:06d}"
        details = vendor_details.get(brand, {
            "country": "US", "city": "Unknown", "currency": "USD", "terms": "NET30"
        })
        
        vendors.append({
            "vendor_id": vendor_id,
            "vendor_name": f"{brand} Corporation",
            "tax_id": f"TAX{random.randint(100000000, 999999999)}",
            "country_code": details["country"],
            "city": details["city"],
            "payment_terms": details["terms"],
            "vendor_type": "SUPPLIER",
            "is_active": True,
            "preferred_currency": details["currency"],
            "credit_limit": random.randint(100000, 800000),
            "street_address": f"{random.randint(1, 999)} Business {random.choice(['Street', 'Avenue', 'Drive', 'Plaza'])}",
            "postal_code": f"{random.randint(10000, 99999)}",
            "contact_email": f"orders@{brand.lower()}.com",
            "contact_phone": f"+{random.randint(1, 99)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "created_date": "2024-01-01",
            "updated_date": f"2025-0{random.randint(6, 8)}-{random.randint(1, 28):02d}"
        })
        
        vendor_id_counter += 1
    
    # Write to CSV
    output_file = RAW_DIR / "vendor_master.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            "vendor_id", "vendor_name", "tax_id", "country_code", "city",
            "payment_terms", "vendor_type", "is_active", "preferred_currency", 
            "credit_limit", "street_address", "postal_code", "contact_email", 
            "contact_phone", "created_date", "updated_date"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(vendors)
    
    print(f"✅ Created {output_file} with {len(vendors)} vendor records")

def generate_exchange_rates_csv():
    """Generate exchange rates CSV for multi-currency support."""
    print("💱 Generating exchange rates CSV...")
    
    # Base currencies and their rates (USD as base)
    base_rates = {
        "USD": 1.0,
        "AUD": 1.52,  # 1 USD = 1.52 AUD
        "EUR": 0.85,  # 1 USD = 0.85 EUR
        "GBP": 0.74,  # 1 USD = 0.74 GBP
        "SGD": 1.34,  # 1 USD = 1.34 SGD
    }
    
    exchange_rates = []
    
    # Generate daily rates for July-August 2025
    current_date = START_DATE
    while current_date <= END_DATE:
        for from_currency, base_rate in base_rates.items():
            for to_currency, to_base_rate in base_rates.items():
                if from_currency != to_currency:
                    # Calculate cross rate with small daily fluctuation
                    cross_rate = to_base_rate / base_rate
                    # Add ±2% daily fluctuation
                    fluctuation = random.uniform(-0.02, 0.02)
                    daily_rate = cross_rate * (1 + fluctuation)
                    
                    exchange_rates.append({
                        "date": current_date.strftime("%Y-%m-%d"),
                        "from_currency": from_currency,
                        "to_currency": to_currency,
                        "exchange_rate": round(daily_rate, 6),
                        "rate_type": "DAILY",
                        "source": "FX_FEED",
                        "is_active": True,
                        "created_timestamp": current_date.strftime("%Y-%m-%d %H:%M:%S")
                    })
        
        current_date += timedelta(days=1)
    
    # Write to CSV
    output_file = RAW_DIR / "exchange_rates.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            "date", "from_currency", "to_currency", "exchange_rate",
            "rate_type", "source", "is_active", "created_timestamp"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(exchange_rates)
    
    print(f"✅ Created {output_file} with {len(exchange_rates)} exchange rate records")

def split_line_items_by_country_cost_centre():
    """Split line items into country/cost centre files with dates, removing data quality flags."""
    print("📂 Splitting line items by country and cost centre...")
    
    # Read line items
    line_items = []
    try:
        with open(SIMULATED_DIR / "line_items.jsonl", 'r') as f:
            for line in f:
                line_items.append(json.loads(line.strip()))
    except FileNotFoundError:
        print("❌ line_items.jsonl not found")
        return
    
    # Group items by country and cost centre
    country_cost_centre_items = defaultdict(list)
    
    for item in line_items:
        # Randomly assign country and cost centre
        country = random.choice(list(COUNTRIES.keys()))
        cost_centre = random.choice(COUNTRIES[country]["cost_centres"])
        
        # Add random date within range
        random_date = START_DATE + timedelta(
            days=random.randint(0, (END_DATE - START_DATE).days)
        )
        
        # Remove data quality flag and clean up the item for production use
        clean_item = {
            "sku_id": item.get("sku_id"),
            "item_description": item.get("item_description"),
            "brand": item.get("brand"),
            "quantity": item.get("quantity"),
            "unit": item.get("unit"),
            "size": item.get("size"),
            # Note: data_quality_flag is intentionally removed here
        }
        
        # Enhance item with additional fields
        enhanced_item = {
            **clean_item,
            "order_date": random_date.strftime("%Y-%m-%d"),
            "order_timestamp": random_date.strftime("%Y-%m-%d %H:%M:%S"),
            "country_code": country,
            "cost_centre_id": cost_centre,
            "currency": COUNTRIES[country]["currency"],
            "unit_price": round(random.uniform(10.0, 500.0), 2),
            "total_amount": None,  # Will be calculated
            "order_id": f"ORD_{country}_{random.randint(100000, 999999)}",
            "line_number": random.randint(1, 10),
            "requisitioner": f"user_{random.randint(1000, 9999)}@company.com",
            "approval_status": random.choice(["APPROVED", "PENDING", "SUBMITTED"]),
            "delivery_date": (random_date + timedelta(days=random.randint(3, 14))).strftime("%Y-%m-%d")
        }
        
        # Calculate total amount
        enhanced_item["total_amount"] = round(
            enhanced_item["unit_price"] * enhanced_item["quantity"], 2
        )
        
        key = f"{country}_{cost_centre}"
        country_cost_centre_items[key].append(enhanced_item)
    
    # Write separate JSON files for each country/cost centre combination
    file_count = 0
    for key, items in country_cost_centre_items.items():
        country, cost_centre = key.split("_", 1)
        
        # Sort by date for better organization
        items.sort(key=lambda x: x["order_date"])
        
        # Create filename with good naming convention
        filename = f"orders_{country.lower()}_{cost_centre.lower()}_{datetime.now().strftime('%Y%m')}.json"
        output_file = RAW_DIR / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
        
        print(f"   📄 Created {filename} with {len(items)} orders")
        file_count += 1
    
    print(f"✅ Split line items into {file_count} country/cost centre files")
    print("   ℹ️  Data quality flags removed from production JSON files")

def main():
    """Main function to generate all SAP ARIBA data lake components."""
    print("🚀 Starting SAP ARIBA Data Lake Generation")
    print("=" * 50)
    
    # # Generate canonical reference data first (from clean line items)
    # # NOTE: Saved in /hidden/ since in real-life, this info doesn't come from
    # # data sources and needs to be generated by hand / inference models
    generate_canonical_brand_table()
    generate_canonical_sku_catalog()
    
    # Generate master data
    generate_cost_centres_csv()
    generate_vendor_master_csv()
    generate_exchange_rates_csv()
    
    # Process line items (removing data quality flags for production)
    split_line_items_by_country_cost_centre()
    
    print("\n" + "=" * 50)
    print("🎉 SAP ARIBA Data Lake Generation Complete!")
    print("\nGenerated files:")
    print("📊 Raw data:")
    for file in RAW_DIR.glob("*"):
        print(f"   - {file.name}")
    print("🤖 Simulated data:")
    for file in SIMULATED_DIR.glob("*"):
        print(f"   - {file.name}")
    print("\n💡 Note: Data quality flags have been removed from production JSON files.")
    print("   Canonical reference tables created from clean data entries.")

if __name__ == "__main__":
    main()
