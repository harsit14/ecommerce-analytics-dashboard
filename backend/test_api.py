"""
Test script for E-Commerce Analytics API
Run this to verify all endpoints are working correctly
"""
import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"

def print_result(feature_name, success, response_time, data=None, error=None):
    """Pretty print test results"""
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"\n{status} {feature_name}")
    print(f"   Response Time: {response_time:.2f}ms")
    if success and data:
        print(f"   Data Preview: {json.dumps(data, indent=2)[:200]}...")
    if error:
        print(f"   Error: {error}")

def test_health_check():
    """Test root endpoint"""
    try:
        start = datetime.now()
        response = requests.get(f"{BASE_URL}/")
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        if response.status_code == 200:
            print_result("Health Check", True, elapsed, response.json())
            return True
        else:
            print_result("Health Check", False, elapsed, error=response.text)
            return False
    except Exception as e:
        print_result("Health Check", False, 0, error=str(e))
        return False

def test_sales_funnel():
    """Test Feature 1: Sales Funnel"""
    try:
        start = datetime.now()
        response = requests.get(f"{BASE_URL}/api/sales-funnel")
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_result("Feature 1: Sales Funnel", True, elapsed, data)
            print(f"   Stages: {len(data.get('funnel', []))}")
            return True
        else:
            print_result("Feature 1: Sales Funnel", False, elapsed, error=response.text)
            return False
    except Exception as e:
        print_result("Feature 1: Sales Funnel", False, 0, error=str(e))
        return False

def test_top_products():
    """Test Feature 2: Top Converting Products"""
    try:
        start = datetime.now()
        response = requests.get(f"{BASE_URL}/api/products/top-converting?limit=5")
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_result("Feature 2: Top Converting Products", True, elapsed)
            print(f"   Products returned: {len(data.get('products', []))}")
            if data.get('products'):
                top = data['products'][0]
                print(f"   Top product: {top.get('product_name')} ({top.get('conversion_rate')}%)")
            return True
        else:
            print_result("Feature 2: Top Converting Products", False, elapsed, error=response.text)
            return False
    except Exception as e:
        print_result("Feature 2: Top Converting Products", False, 0, error=str(e))
        return False

def test_abandoned_carts():
    """Test Feature 3: Abandoned Cart Analysis"""
    try:
        start = datetime.now()
        response = requests.get(f"{BASE_URL}/api/products/abandoned-carts?limit=5")
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_result("Feature 3: Abandoned Cart Analysis", True, elapsed)
            print(f"   Products returned: {len(data.get('products', []))}")
            if data.get('products'):
                top = data['products'][0]
                print(f"   Most abandoned: {top.get('product_name')} ({top.get('abandonment_count')} carts)")
            return True
        else:
            print_result("Feature 3: Abandoned Cart Analysis", False, elapsed, error=response.text)
            return False
    except Exception as e:
        print_result("Feature 3: Abandoned Cart Analysis", False, 0, error=str(e))
        return False

def test_session_analytics():
    """Test Feature 4: Session Analytics"""
    try:
        start = datetime.now()
        response = requests.get(f"{BASE_URL}/api/sessions/analytics")
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_result("Feature 4: Session Analytics", True, elapsed)
            print(f"   Segments: {len(data.get('segments', []))}")
            for segment in data.get('segments', []):
                print(f"   - {segment.get('user_segment')}: "
                      f"{segment.get('avg_events_per_session'):.2f} events/session")
            return True
        else:
            print_result("Feature 4: Session Analytics", False, elapsed, error=response.text)
            return False
    except Exception as e:
        print_result("Feature 4: Session Analytics", False, 0, error=str(e))
        return False

def test_brand_trends():
    """Test Feature 5: Brand Popularity Trends"""
    try:
        start = datetime.now()
        response = requests.get(f"{BASE_URL}/api/brands/trends?brand=samsung")
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        if response.status_code == 200:
            data = response.json()
            print_result("Feature 5: Brand Popularity Trends", True, elapsed)
            print(f"   Brand: {data.get('brand')}")
            print(f"   Records: {data.get('total_records')}")
            print(f"   Date range: {data.get('date_range')}")
            return True
        else:
            print_result("Feature 5: Brand Popularity Trends", False, elapsed, error=response.text)
            return False
    except Exception as e:
        print_result("Feature 5: Brand Popularity Trends", False, 0, error=str(e))
        return False

def main():
    """Run all tests"""
    print("=" * 70)
    print("🧪 E-Commerce Analytics API Test Suite")
    print("=" * 70)
    print(f"Testing API at: {BASE_URL}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    tests = [
        ("Health Check", test_health_check),
        ("Feature 1", test_sales_funnel),
        ("Feature 2", test_top_products),
        ("Feature 3", test_abandoned_carts),
        ("Feature 4", test_session_analytics),
        ("Feature 5", test_brand_trends),
    ]
    
    results = []
    for name, test_func in tests:
        results.append(test_func())
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    print(f"Failed: {total - passed}/{total}")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Backend is ready for frontend integration!")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
