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
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from collections import defaultdict

# Configuration
DATA_DIR = Path(__file__).parent
RAW_DIR = DATA_DIR.parent / "source"
SIMULATED_DIR = DATA_DIR / "output"

# Ensure directories exist
RAW_DIR.mkdir(exist_ok=True)
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

def generate_vendor_master_csv():
    """Generate vendor master CSV - one entry per vendor (SAP standard)."""
    print("🏭 Generating vendor master CSV...")
    
    # Read product SKUs to extract brands
    brands = set()
    try:
        with open(SIMULATED_DIR / "product_skus.jsonl", 'r') as f:
            for line in f:
                sku = json.loads(line.strip())
                brands.add(sku["brand"])
    except FileNotFoundError:
        print("⚠️  product_skus.jsonl not found, using default brands")
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
    """Split line items into country/cost centre files with dates."""
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
        
        # Enhance item with additional fields
        enhanced_item = {
            **item,
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

def main():
    """Main function to generate all SAP ARIBA data lake components."""
    print("🚀 Starting SAP ARIBA Data Lake Generation")
    print("=" * 50)
    
    # Generate master data
    generate_cost_centres_csv()
    generate_vendor_master_csv()
    generate_exchange_rates_csv()
    
    # Process line items
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

if __name__ == "__main__":
    main()
