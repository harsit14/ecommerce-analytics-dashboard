# E-Commerce Analytics Dashboard

A full-stack analytics platform for analyzing 200M+ e-commerce user behavior events from October 2019 to April 2020.

## 🌐 Live Demo

- **Dashboard**: https://databaseproject14.netlify.app
- **API Documentation**: https://ecommerce-api-398354112983.us-central1.run.app/docs

## 📊 Project Overview

This project implements a high-performance analytics system that processes a massive Kaggle dataset containing over 200 million user interaction events. The system provides five key analytical features with sub-second query response times through strategic use of partitioning, indexing, and materialized views.

**Dataset**: [E-Commerce Behavior Data from Multi-Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- 200M+ events (views, carts, purchases)
- 4.5M+ users
- 240K+ products
- ~52GB raw CSV data

## 🏗️ Architecture

```
Frontend (React)     →     Backend (FastAPI)     →     Database (PostgreSQL)
   Netlify                 Google Cloud Run           Google Cloud SQL
```

## 🛠️ Tech Stack

### Frontend
- React 18
- Material-UI v5
- Recharts
- Axios

### Backend
- FastAPI
- Python 3.11
- PostgreSQL driver (psycopg2)
- Pydantic

### Database
- PostgreSQL 14
- Google Cloud SQL
- Partitioned tables (7 monthly partitions)
- 5 Materialized views

## ✨ Features

1. **Sales Funnel Visualization** - View → Cart → Purchase conversion metrics
2. **Product Conversion Rate Leaderboard** - Top products by conversion rate
3. **Abandoned Cart Analysis** - Most frequently abandoned products
4. **User Session Analytics** - Purchaser vs non-purchaser comparison
5. **Brand Popularity Trends** - Time-series brand performance data

## 📁 Repository Structure

```
ecommerce_analytics/
├── backend/
│   ├── main.py              # FastAPI application
│   ├── database.py          # Database connection pool
│   ├── models.py            # Pydantic models
│   ├── Dockerfile           # Container configuration
│   ├── requirements.txt     # Python dependencies
│   └── .env.example         # Environment variables template
│
├── frontend/
│   ├── public/              # Static assets
│   ├── src/
│   │   ├── components/      # React components (5 features)
│   │   ├── services/        # API service layer
│   │   ├── App.js           # Main application
│   │   └── App.css          # Styling
│   ├── package.json         # Node dependencies
│   └── .env.example         # Environment variables template
│
├── scripts/                 # Database setup scripts (optional)
├── docs/                    # Project documentation
└── README.md               # This file
```

## 🚀 Quick Start (Using Live Deployment)

The easiest way to test this project is to use the live deployment:

1. **Visit Dashboard**: https://databaseproject14.netlify.app
2. **Explore API**: https://ecommerce-api-398354112983.us-central1.run.app/docs
3. **Test Features**: Navigate through all 5 tabs in the dashboard

## 💻 Local Development Setup

### Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with database credentials (use deployed DB or your own)

# Run API
uvicorn main:app --reload --port 8000
```

Backend will be available at `http://localhost:8000`

### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# (Optional) Configure API URL for local backend
# Edit src/services/apiService.js line 7:
# const API_BASE_URL = 'http://localhost:8000';

# Start development server
npm start
```

Frontend will open at `http://localhost:3000`

## 🗄️ Database Access

### Option 1: Use Deployed Database (Recommended)

The deployed API already connects to a fully populated database with 200M+ events. Simply use the API endpoints or interact via the live dashboard.

### Option 2: Local Database Setup

If you want to replicate the full database:

1. Download dataset from Kaggle (~52GB)
2. Create PostgreSQL database
3. Run ETL pipeline (8-12 hours)
4. Create materialized views

**Note**: Contact project team for detailed database setup scripts if needed.

## 🧪 API Endpoints

| Endpoint | Description | Example |
|----------|-------------|---------|
| `GET /api/sales-funnel` | Sales funnel metrics | [Try it](https://ecommerce-api-398354112983.us-central1.run.app/api/sales-funnel) |
| `GET /api/products/top-converting` | Top converting products | [Try it](https://ecommerce-api-398354112983.us-central1.run.app/api/products/top-converting?limit=10) |
| `GET /api/products/abandoned-carts` | Abandoned cart analysis | [Try it](https://ecommerce-api-398354112983.us-central1.run.app/api/products/abandoned-carts?limit=10) |
| `GET /api/sessions/analytics` | Session analytics | [Try it](https://ecommerce-api-398354112983.us-central1.run.app/api/sessions/analytics) |
| `GET /api/brands/trends` | Brand trends | [Try it](https://ecommerce-api-398354112983.us-central1.run.app/api/brands/trends?brand=samsung) |

## 🌐 Deployment

### Deploy Backend to Google Cloud Run

```bash
cd backend

gcloud run deploy ecommerce-api \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --timeout=300 \
  --set-env-vars DB_HOST=YOUR_DB_HOST,DB_PORT=5432,DB_NAME=ecommerce_analytics,DB_USER=postgres,DB_PASSWORD=YOUR_PASSWORD
```

### Deploy Frontend to Netlify

```bash
cd frontend
npm run build
netlify deploy --prod --dir=build
```

## 📈 Performance Metrics

- **Query Response Time**: <1 second (all endpoints)
- **Database Size**: ~45GB
- **Total Events**: 200,847,064
- **Frontend Load Time**: 1-2 seconds
- **API Cold Start**: 2-5 seconds
- **API Warm Requests**: <500ms

## 🗄️ Database Schema

### Tables
- **events**: 200M+ rows (partitioned by month)
- **users**: 4.5M rows
- **products**: 240K rows
- **sessions**: 9M rows
- **brands**: ~500 rows
- **categories**: ~300 rows

### Materialized Views (for fast queries)
- `mv_sales_funnel`
- `mv_product_conversion_rates`
- `mv_abandoned_carts`
- `mv_user_session_analytics`
- `mv_brand_popularity_trends`

## 🔐 Environment Variables

### Backend `.env`
```env
DB_HOST=your_database_host
DB_PORT=5432
DB_NAME=ecommerce_analytics
DB_USER=postgres
DB_PASSWORD=your_password
```

### Frontend `.env` (optional)
```env
REACT_APP_API_URL=http://localhost:8000
```

## 🧪 Testing

### Test API Endpoints
```bash
# Health check
curl https://ecommerce-api-398354112983.us-central1.run.app/

# Sales funnel
curl https://ecommerce-api-398354112983.us-central1.run.app/api/sales-funnel
```

### Test Frontend Locally
```bash
cd frontend
npm start
# Visit http://localhost:3000
```

## 📸 Features Overview

1. **Sales Funnel** - Interactive bar chart showing conversion from views to purchases
2. **Top Products** - Sortable table with conversion rates and filters
3. **Abandoned Carts** - Products with highest abandonment rates
4. **Session Analytics** - Comparison charts for different user segments
5. **Brand Trends** - Time-series line charts for brand performance

## 👥 Course Information

**Course**: CS 554 - Advanced Database Systems  
**Project**: Final Project - Milestone 4  
**Date**: Fall 2024

## 🙏 Acknowledgments

- Dataset source: Kaggle E-Commerce Behavior Data
- Cloud infrastructure: Google Cloud Platform
- Frontend hosting: Netlify

## 📞 Support

For questions about replicating this project:
1. Try the live demo first: https://databaseproject14.netlify.app
2. Check API documentation: https://ecommerce-api-398354112983.us-central1.run.app/docs
3. Review this README for setup instructions

---

**Note for Reviewers**: The live deployment is fully functional and demonstrates all required features. The system processes 200M+ records with sub-second query times through strategic database design including partitioning, indexing, and materialized views.
