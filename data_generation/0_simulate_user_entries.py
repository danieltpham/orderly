import json
import asyncio
from pathlib import Path
from pydantic_ai import Agent
from models.schema import ProcurementDataset
from dotenv import load_dotenv
load_dotenv()

# File paths
BASE_DIR = Path(__file__).parent
PROMPT_PATH = BASE_DIR / "prompt" / "data_generation_prompt.txt"
OUTPUT_PATH_SKUS = BASE_DIR / "output" / "product_skus.jsonl"
OUTPUT_PATH_LINEITEMS = BASE_DIR / "output" / "line_items.jsonl"

def load_prompt() -> str:
    """Load the system prompt from file."""
    return PROMPT_PATH.read_text()

# Create the Pydantic AI agent
procurement_agent = Agent(
    'openai:gpt-4o',  # You can also use 'anthropic:claude-3-sonnet' or other models
    output_type=ProcurementDataset,
    system_prompt=load_prompt(),
)   

def save_data(dataset: ProcurementDataset) -> None:
    """Save the dataset to separate JSONL files."""
    
    # Save Product SKUs (source of truth)
    with OUTPUT_PATH_SKUS.open("w") as f:
        for sku in dataset.product_skus:
            f.write(json.dumps(sku.model_dump()) + "\n")
    print(f"✅ Saved {len(dataset.product_skus)} product SKUs to {OUTPUT_PATH_SKUS}")
    
    # Save Line Items (messy user inputs)
    with OUTPUT_PATH_LINEITEMS.open("w") as f:
        for item in dataset.line_items:
            f.write(json.dumps(item.model_dump()) + "\n")
    print(f"✅ Saved {len(dataset.line_items)} line items to {OUTPUT_PATH_LINEITEMS}")

async def generate_dataset() -> None:
    """Generate the complete fuzzy matching dataset using Pydantic AI."""
    
    print("🤖 Generating procurement dataset using Pydantic AI...")
    
    # Target numbers
    TARGET_SKUS = 50
    TARGET_LINE_ITEMS = 500
    
    all_skus = []
    all_line_items = []
    
    iteration = 1
    
    while len(all_skus) < TARGET_SKUS or len(all_line_items) < TARGET_LINE_ITEMS:
        print(f"\n🔄 Generation iteration {iteration}")
        print(f"   Current: {len(all_skus)} SKUs, {len(all_line_items)} line items")
        print(f"   Target:  {TARGET_SKUS} SKUs, {TARGET_LINE_ITEMS} line items")
        
        # Calculate how many more we need
        skus_needed = max(0, TARGET_SKUS - len(all_skus))
        items_needed = max(0, TARGET_LINE_ITEMS - len(all_line_items))
        
        # Generate a smaller batch to avoid overwhelming the model
        batch_skus = min(20, skus_needed) if skus_needed > 0 else 0
        batch_items = min(100, items_needed) if items_needed > 0 else 0
        
        # Skip if we don't need more of either
        if batch_skus == 0 and batch_items == 0:
            break
            
        user_query = f"""Generate a batch of procurement data:
        
        - Generate {batch_skus} new clean product SKUs (if needed)
        - Generate {batch_items} messy line items (if needed)
        
        For line items, include realistic data quality variations:
        - Clean entries that match exactly
        - Fuzzy entries with abbreviations, typos, and missing words  
        - HTML entries with encoding artifacts
        - Non-product entries for fees and services (about 10%)
        
        Use fictional brands like TechFlow, ConnectPro, ViewMaster, DataVault, OfficeLink.
        Categories: Keyboards, Monitors, Cables, Webcams, Headsets, Adapters, Storage.
        
        Ensure line items reference the SKUs you create (except non-product entries with null sku_id)."""
        
        try:
            # Run the agent to generate structured data
            result = await procurement_agent.run(user_query)
            dataset = result.output
            
            # Add new SKUs (avoid duplicates by sku_id)
            existing_sku_ids = {sku.sku_id for sku in all_skus}
            new_skus = [sku for sku in dataset.product_skus if sku.sku_id not in existing_sku_ids]
            all_skus.extend(new_skus)
            
            # Add new line items
            all_line_items.extend(dataset.line_items)
            
            print(f"   ✅ Added {len(new_skus)} SKUs, {len(dataset.line_items)} line items")
            
        except Exception as e:
            print(f"   ❌ Error in iteration {iteration}: {e}")
            if iteration > 10:  # Prevent infinite loops
                print("   🛑 Too many iterations, stopping")
                break
        
        iteration += 1
        
        # Safety check to prevent infinite loops
        if iteration > 15:
            print("🛑 Reached maximum iterations, stopping")
            break
    
    # Create final dataset
    final_dataset = ProcurementDataset(
        product_skus=all_skus,
        line_items=all_line_items
    )
    
    print(f"\n📝 Final validation and cleanup...")
    
    # Validate data consistency
    sku_ids = {sku.sku_id for sku in final_dataset.product_skus}
    line_item_skus = {item.sku_id for item in final_dataset.line_items if item.sku_id is not None}
    
    # Check for orphaned line items and clean them
    orphaned = line_item_skus - sku_ids
    if orphaned:
        print(f"⚠️  Warning: Found {len(orphaned)} line items referencing non-existent SKUs")
        # Filter out orphaned line items (except non-product entries)
        cleaned_items = []
        for item in final_dataset.line_items:
            if item.sku_id is None or item.sku_id in sku_ids or item.sku_id == "":
                if item.sku_id == "":  # Fix empty string SKU IDs
                    item.sku_id = None
                cleaned_items.append(item)
        final_dataset.line_items = cleaned_items
        print(f"   🧹 Cleaned dataset: {len(final_dataset.line_items)} valid line items")
    
    # Save the validated dataset
    save_data(final_dataset)
    
    # Print summary statistics
    quality_counts = {}
    for item in final_dataset.line_items:
        quality_counts[item.data_quality_flag] = quality_counts.get(item.data_quality_flag, 0) + 1
    
    print("\n📊 Final Dataset Statistics:")
    print(f"   Clean SKUs: {len(final_dataset.product_skus)}")
    print(f"   Total Line Items: {len(final_dataset.line_items)}")
    for quality, count in quality_counts.items():
        percentage = (count / len(final_dataset.line_items)) * 100
        print(f"   {quality.capitalize()}: {count} ({percentage:.1f}%)")
    
    print("\n🎉 Dataset generation completed successfully!")
    print("📚 Ready for fuzzy matching and data cleaning model training!")

def main():
    """Main synchronous entry point."""
    asyncio.run(generate_dataset())

if __name__ == "__main__":
    main()
