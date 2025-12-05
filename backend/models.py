"""
Pydantic models for API request/response validation
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date

# Feature 1: Sales Funnel
class SalesFunnelStage(BaseModel):
    stage: str = Field(..., description="Funnel stage (view, cart, purchase)")
    event_count: int = Field(..., description="Total number of events")
    unique_users: int = Field(..., description="Number of unique users")
    
class SalesFunnelResponse(BaseModel):
    funnel: List[SalesFunnelStage]
    
# Feature 2: Product Conversion Rates
class ProductConversion(BaseModel):
    product_id: int
    product_name: str
    category: str
    brand: str
    price: float
    views: int
    carts: int
    purchases: int
    conversion_rate: float = Field(..., description="Purchase/View ratio")
    
class ProductConversionResponse(BaseModel):
    products: List[ProductConversion]
    total_count: int
    
# Feature 3: Abandoned Carts
class AbandonedCartProduct(BaseModel):
    product_id: int
    product_name: str
    category: str
    brand: str
    price: float
    cart_adds: int
    purchases: int
    abandonment_count: int
    abandonment_rate: float = Field(..., description="Percentage of carts abandoned")
    
class AbandonedCartResponse(BaseModel):
    products: List[AbandonedCartProduct]
    total_count: int
    
# Feature 4: User Session Analytics
class SessionAnalytics(BaseModel):
    user_segment: str = Field(..., description="User type (purchasers, non_purchasers, all_users)")
    avg_session_duration_seconds: float
    avg_events_per_session: float
    total_sessions: int
    total_users: int
    
class SessionAnalyticsResponse(BaseModel):
    segments: List[SessionAnalytics]
    
# Feature 5: Brand Popularity Trends
class BrandTrend(BaseModel):
    date: date
    brand: str
    views: int
    carts: int
    purchases: int
    revenue: float
    unique_users: int
    
class BrandTrendsResponse(BaseModel):
    trends: List[BrandTrend]
    brand: str
    total_records: int
    date_range: dict

# Error response model
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
