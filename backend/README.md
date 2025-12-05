# E-Commerce Analytics Backend API

FastAPI backend for querying 200M+ e-commerce behavior events.

## 🚀 Quick Start (Local Development)

### Prerequisites
- Python 3.8+
- Access to database at 104.198.184.12

### Setup

1. **Create and activate virtual environment:**
   ```bash
   python -m venv venv
   
   # Windows
   venv\Scripts\activate
   
   # Mac/Linux
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create `.env` file:**
   ```env
   DB_HOST=104.198.184.12
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=your_password_here
   DB_NAME=ecommerce_analytics
   ```

4. **Run the API:**
   ```bash
   uvicorn main:app --reload --port 8000
   ```

5. **Visit API documentation:**
   - Swagger UI: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

## 📋 API Endpoints

### Feature 1: Sales Funnel
```
GET /api/sales-funnel
Returns: View → Cart → Purchase funnel metrics
```

### Feature 2: Top Converting Products
```
GET /api/products/top-converting?limit=20
Returns: Products with highest conversion rates
```

### Feature 3: Abandoned Cart Analysis
```
GET /api/products/abandoned-carts?limit=20
Returns: Most abandoned products
```

### Feature 4: Session Analytics
```
GET /api/sessions/analytics
Returns: Comparison of purchasers vs non-purchasers
```

### Feature 5: Brand Trends
```
GET /api/brands/trends?brand=samsung&start_date=2019-10-01&end_date=2020-04-30
Returns: Daily brand performance metrics
```

## 🧪 Testing

### Test with curl:
```bash
# Sales Funnel
curl http://localhost:8000/api/sales-funnel

# Top Products
curl http://localhost:8000/api/products/top-converting?limit=10

# Abandoned Carts
curl http://localhost:8000/api/products/abandoned-carts?limit=10

# Session Analytics
curl http://localhost:8000/api/sessions/analytics

# Brand Trends
curl "http://localhost:8000/api/brands/trends?brand=samsung"
```

### Test with Python:
```python
import requests

# Sales funnel
response = requests.get('http://localhost:8000/api/sales-funnel')
print(response.json())
```

## 📦 Project Structure

```
backend/
├── main.py              # FastAPI application
├── database.py          # Database connection pool
├── models.py            # Pydantic response models
├── requirements.txt     # Dependencies
├── .env                 # Environment variables (create this)
└── README.md           # This file
```

## 🚀 Deployment to Google Cloud Run

### 1. Create Dockerfile
See `Dockerfile` in this directory.

### 2. Build and push image:
```bash
gcloud builds submit --tag gcr.io/database-project-477917/ecommerce-api
```

### 3. Deploy to Cloud Run:
```bash
gcloud run deploy ecommerce-api \
  --image gcr.io/database-project-477917/ecommerce-api \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars DB_HOST=104.198.184.12,DB_PORT=5432,DB_NAME=ecommerce_analytics \
  --set-secrets DB_USER=postgres-user:latest,DB_PASSWORD=postgres-password:latest
```

## 🔒 Security Notes

- **Production**: Change CORS `allow_origins` to specific frontend URL
- **Production**: Use Cloud SQL Proxy or Private IP
- **Production**: Store credentials in Secret Manager
- **Production**: Enable authentication on endpoints

## 📊 Performance

All endpoints return in <1 second:
- Sales Funnel: ~50-100ms
- Product queries: ~200-500ms
- Session Analytics: ~100-200ms
- Brand Trends: ~500ms-2s (depending on date range)

## 🐛 Troubleshooting

**Connection refused:**
- Check database is running
- Verify `.env` credentials
- Check firewall/authorized networks

**Slow queries:**
- Materialized views should make queries fast
- If slow, contact database admin to refresh views

**Module not found:**
- Make sure virtual environment is activated
- Run `pip install -r requirements.txt`

## 📝 Next Steps

1. ✅ Backend API complete
2. 🚧 Build React frontend
3. 🚧 Integrate frontend with API
4. 🚧 Deploy both to production
5. 🚧 Take screenshots for report

---

**Status**: ✅ Ready for frontend integration  
**Database**: 200M+ events, <1s query time  
**Last Updated**: December 2025
