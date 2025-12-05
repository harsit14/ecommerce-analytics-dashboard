"""
E-Commerce Analytics API
FastAPI backend for querying 200M+ event dataset
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from datetime import date
from typing import Optional

from database import init_connection_pool, close_connection_pool, get_db_cursor
from models import (
    SalesFunnelResponse, 
    ProductConversionResponse,
    AbandonedCartResponse,
    SessionAnalyticsResponse,
    BrandTrendsResponse,
    ErrorResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="E-Commerce Analytics API",
    description="High-performance analytics API for 200M+ user behavior events",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware - configured for both development and production
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # Local development
        "https://*.netlify.app",  # Netlify deployments (all subdomains)
        # Add your production URL here after deployment:
        # "https://your-app-name.netlify.app",
        "https://databaseproject14.netlify.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize API on startup"""
    logger.info("Starting E-Commerce Analytics API...")
    # Don't initialize DB pool here - it will be initialized on first request
    # This prevents startup timeout issues on Cloud Run
    logger.info("✅ API ready to serve requests")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections on shutdown"""
    logger.info("Shutting down API...")
    close_connection_pool()
    logger.info("✅ Shutdown complete")

# Root endpoint
@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "E-Commerce Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "docs": "/docs",
            "sales_funnel": "/api/sales-funnel",
            "top_products": "/api/products/top-converting",
            "abandoned_carts": "/api/products/abandoned-carts",
            "session_analytics": "/api/sessions/analytics",
            "brand_trends": "/api/brands/trends"
        }
    }

# Feature 1: Sales Funnel Visualization
@app.get(
    "/api/sales-funnel",
    response_model=SalesFunnelResponse,
    tags=["Analytics"],
    summary="Get sales funnel metrics",
    description="Returns view → cart → purchase conversion funnel"
)
async def get_sales_funnel():
    """
    Get sales funnel metrics showing the customer journey.
    
    Returns:
    - Total events for each stage (view, cart, purchase)
    - Unique users at each stage
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    event_type,
                    event_count,
                    unique_users
                FROM mv_sales_funnel
                ORDER BY 
                    CASE event_type
                        WHEN 'view' THEN 1
                        WHEN 'cart' THEN 2
                        WHEN 'purchase' THEN 3
                    END
            """)
            
            results = cursor.fetchall()
            
            funnel_data = [
                {
                    "stage": row[0],
                    "event_count": int(row[1]),
                    "unique_users": int(row[2])
                }
                for row in results
            ]
            
            logger.info(f"✅ Sales funnel query returned {len(funnel_data)} stages")
            return {"funnel": funnel_data}
            
    except Exception as e:
        logger.error(f"❌ Error in sales funnel: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Feature 2: Product Conversion Rate Leaderboard
@app.get(
    "/api/products/top-converting",
    response_model=ProductConversionResponse,
    tags=["Products"],
    summary="Get top converting products",
    description="Returns products with highest purchase/view conversion rates"
)
async def get_top_converting_products(
    limit: int = Query(20, ge=1, le=100, description="Number of products to return")
):
    """
    Get products with the highest conversion rates.
    
    Args:
    - limit: Number of products to return (1-100, default 20)
    
    Returns:
    - List of products with conversion metrics
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    product_id,
                    brand_name,
                    category_level_1,
                    category_level_2,
                    current_price,
                    total_views,
                    total_carts,
                    total_purchases,
                    conversion_rate
                FROM mv_product_conversion_rates
                ORDER BY conversion_rate DESC
                LIMIT %s
            """, (limit,))
            
            results = cursor.fetchall()
            
            products = [
                {
                    "product_id": row[0],
                    "product_name": f"Product {row[0]}",  # No product name in view
                    "category": (f"{row[2]}/{row[3]}" if row[2] and row[3] 
                               else (row[2] or row[3] or "Uncategorized")),
                    "brand": row[1] or "Unknown",
                    "price": float(row[4]),
                    "views": int(row[5]),
                    "carts": int(row[6]),
                    "purchases": int(row[7]),
                    "conversion_rate": float(row[8])
                }
                for row in results
            ]
            
            logger.info(f"✅ Top converting products query returned {len(products)} products")
            return {"products": products, "total_count": len(products)}
            
    except Exception as e:
        logger.error(f"❌ Error in top converting products: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Feature 3: Abandoned Cart Analysis
@app.get(
    "/api/products/abandoned-carts",
    response_model=AbandonedCartResponse,
    tags=["Products"],
    summary="Get most abandoned cart products",
    description="Returns products most frequently added to cart but not purchased"
)
async def get_abandoned_cart_products(
    limit: int = Query(20, ge=1, le=100, description="Number of products to return")
):
    """
    Get products with the highest cart abandonment.
    
    Args:
    - limit: Number of products to return (1-100, default 20)
    
    Returns:
    - List of products with abandonment metrics
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    product_id,
                    brand_name,
                    category_level_1,
                    category_level_2,
                    current_price,
                    total_carts,
                    total_purchases,
                    abandoned_count,
                    abandonment_rate
                FROM mv_abandoned_carts
                ORDER BY abandoned_count DESC
                LIMIT %s
            """, (limit,))
            
            results = cursor.fetchall()
            
            products = [
                {
                    "product_id": row[0],
                    "product_name": f"Product {row[0]}",  # No product name in view
                    "category": (f"{row[2]}/{row[3]}" if row[2] and row[3] 
                               else (row[2] or row[3] or "Uncategorized")),
                    "brand": row[1] or "Unknown",
                    "price": float(row[4]),
                    "cart_adds": int(row[5]),
                    "purchases": int(row[6]),
                    "abandonment_count": int(row[7]),
                    "abandonment_rate": float(row[8])
                }
                for row in results
            ]
            
            logger.info(f"✅ Abandoned carts query returned {len(products)} products")
            return {"products": products, "total_count": len(products)}
            
    except Exception as e:
        logger.error(f"❌ Error in abandoned carts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Feature 4: User Session Analytics
@app.get(
    "/api/sessions/analytics",
    response_model=SessionAnalyticsResponse,
    tags=["Analytics"],
    summary="Get user session analytics",
    description="Compare session metrics between purchasers and non-purchasers"
)
async def get_session_analytics():
    """
    Get comparative session analytics.
    
    Returns:
    - Average session duration and events per session
    - Comparison between purchasers, non-purchasers, and all users
    """
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    user_type,
                    avg_session_duration_seconds,
                    avg_events_per_session,
                    session_count,
                    user_count
                FROM mv_user_session_analytics
                ORDER BY 
                    CASE user_type
                        WHEN 'purchasers' THEN 1
                        WHEN 'non_purchasers' THEN 2
                        WHEN 'all_users' THEN 3
                    END
            """)
            
            results = cursor.fetchall()
            
            segments = [
                {
                    "user_segment": row[0],
                    "avg_session_duration_seconds": float(row[1]),
                    "avg_events_per_session": float(row[2]),
                    "total_sessions": int(row[3]),
                    "total_users": int(row[4])
                }
                for row in results
            ]
            
            logger.info(f"✅ Session analytics query returned {len(segments)} segments")
            return {"segments": segments}
            
    except Exception as e:
        logger.error(f"❌ Error in session analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Feature 5: Brand Popularity Trends
@app.get(
    "/api/brands/trends",
    response_model=BrandTrendsResponse,
    tags=["Analytics"],
    summary="Get brand popularity trends",
    description="Returns time-series data for brand performance metrics"
)
async def get_brand_trends(
    brand: str = Query(..., description="Brand name (e.g., 'samsung', 'apple')"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get daily brand performance trends.
    
    Args:
    - brand: Brand name (required)
    - start_date: Optional start date filter
    - end_date: Optional end date filter
    
    Returns:
    - Daily metrics for the specified brand
    """
    try:
        with get_db_cursor() as cursor:
            # Build query with optional date filters
            query = """
                SELECT 
                    date,
                    brand,
                    views,
                    carts,
                    purchases,
                    revenue,
                    unique_users
                FROM mv_brand_popularity_trends
                WHERE LOWER(brand) = LOWER(%s)
            """
            params = [brand]
            
            if start_date:
                query += " AND date >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= %s"
                params.append(end_date)
            
            query += " ORDER BY date"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No data found for brand '{brand}'"
                )
            
            trends = [
                {
                    "date": row[0],
                    "brand": row[1],
                    "views": int(row[2]),
                    "carts": int(row[3]),
                    "purchases": int(row[4]),
                    "revenue": float(row[5]),
                    "unique_users": int(row[6])
                }
                for row in results
            ]
            
            # Calculate date range
            date_range = {
                "start": str(trends[0]["date"]),
                "end": str(trends[-1]["date"])
            }
            
            logger.info(f"✅ Brand trends query returned {len(trends)} records for {brand}")
            return {
                "trends": trends,
                "brand": brand,
                "total_records": len(trends),
                "date_range": date_range
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in brand trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )

# Run with: uvicorn main:app --reload --port 8000
if __name__ == "__main__":
    import uvicorn
    import os
    # Cloud Run sets PORT environment variable
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
