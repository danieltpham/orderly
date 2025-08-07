from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum

class DataQualityFlag(str, Enum):
    """Data quality classification for line items."""
    CLEAN = "clean"           # Clear, professional entry
    FUZZY = "fuzzy"          # Abbreviated, typos, casual entry
    HTML = "html"            # HTML artifacts or encoding issues
    NON_PRODUCT = "non_product"  # Not a product (fees, services, etc.)

class ProductSKU(BaseModel):
    """
    Clean, standardized product record - the "source of truth".
    This represents the canonical product information that messy line items should match to.
    """
    sku_id: str = Field(..., description="Unique SKU identifier (e.g., 'SKU001')")
    brand: str = Field(..., description="Official brand name (standardized)")
    product_name: str = Field(..., description="Clean, standardized product name")
    variant: Optional[str] = Field(None, description="Product variant (color, size, model, etc.)")
    category: str = Field(..., description="Product category (e.g., 'Keyboards', 'Monitors', 'Cables')")
    unit: str = Field(default="each", description="Standard unit of measure")
    size: Optional[str] = Field(None, description="Standardized size/dimensions")
    
    def get_canonical_description(self) -> str:
        """Generate the canonical description for this SKU."""
        parts = [self.brand, self.product_name]
        if self.variant:
            parts.append(f"- {self.variant}")
        if self.size:
            parts.append(f"({self.size})")
        return " ".join(parts)

class LineItem(BaseModel):
    """
    Messy user-input line item that should match to a clean ProductSKU.
    Represents how items appear in real purchase orders with inconsistencies.
    """
    # Reference to the clean SKU (ground truth for training)
    sku_id: Optional[str] = Field(None, description="Reference to the clean ProductSKU this should match to (null for non-product entries)")
    
    # Messy user input
    item_description: str = Field(..., description="Raw item description as entered by user")
    brand: Optional[str] = Field(None, description="Brand name as entered (may be inconsistent/missing)")
    quantity: int = Field(..., description="Quantity ordered", gt=0)
    unit: Optional[str] = Field(None, description="Unit of measure as entered (may be missing/wrong)")
    size: Optional[str] = Field(None, description="Size as entered (may be inconsistent/missing)")
    
    # Data quality classification
    data_quality_flag: DataQualityFlag = Field(..., description="Quality classification of this entry")

class ProcurementDataset(BaseModel):
    """
    Complete dataset containing both clean SKUs and messy line items.
    This is the structured output from the Pydantic AI agent.
    """
    product_skus: List[ProductSKU] = Field(..., description="List of clean product SKUs (source of truth)")
    line_items: List[LineItem] = Field(..., description="List of messy line items that reference the SKUs")
