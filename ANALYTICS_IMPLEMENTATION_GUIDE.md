# Analytics Dashboard Implementation Guide

## Overview

This guide documents the implementation of the analytics dashboard with 188 analytics across 42 categories, including the re-inference button and auto-display after pipeline completion.

## Progress Status

### ✅ Phase 1: File Organization (COMPLETE)
- Moved streaming ML files to `machine-learning/` directory
- Cleaned up root directory
- Updated all imports

### ✅ Phase 2: Analytics API Backend (COMPLETE)
- Created `AnalyticsService` with parquet file support
- Added 6 API endpoints for fetching analytics
- Implemented caching strategy (5-minute TTL)
- Support for 188 analytics in 42 categories

### 🔄 Phase 3: Re-Inference API (IN PROGRESS)
Backend changes needed for re-inference button

### 🔄 Phase 4: Frontend Analytics Dashboard (TODO)
React components for displaying 188 analytics

### 🔄 Phase 5: Auto-Display After Pipeline (TODO)
Auto-navigate and display analytics when pipeline completes

---

## Analytics Categories (188 Total)

### Customer & General Analytics (130 items)

1. **Business Health (4)**
   - business_health_daily, weekly, monthly
   - low_margin_categories

2. **Customer Acquisition & Growth (12)**
   - new_customers (daily, weekly, monthly)
   - cumulative_customers (daily, weekly, monthly)
   - customer_account_status_distribution (daily, weekly, monthly)
   - geo_acquisition, new_customers_geo_acquisition

3. **Customer Demographics (5)**
   - customer_age_group_distribution
   - customer_city/state/country_distribution
   - customer_age_group_spending

4. **Customer Preferences (2)**
   - gender_category_preference
   - gender_product_preference

5. **Customer Segmentation (6)**
   - new_vs_returning_customer (country, city, state)
   - rfm_segment_summary
   - customer_overall_health_summary
   - high_intent_non_buyers

6. **Customer Engagement (3)**
   - customer_engagement, summary
   - session_to_order_analysis

7. **Customer Value (11)**
   - top_customers_by_revenue/profit
   - clv_summary
   - customer_profit_per_segment
   - segment_aov_by_rfm
   - cart/session analytics
   - discount analysis

8. **Revenue Analysis (6)**
   - rev_by_country_city, customer_segment
   - rev_by_rfm_segment, segment_label
   - rev_by_referrer, device

9. **AOV Trends (3)**
   - aov_trend_daily, weekly, monthly

10. **Churn & Risk (2)**
    - churn_risk_summary
    - high_clv_at_risk

11. **Cohort Analysis (3)**
    - customers_cohorts
    - signup_cohort_summary
    - customer_cohort_retention

12. **Cross-dimensional (3)**
    - rfm_churn_crosstab
    - seg_referrer/device_crosstab

13. **Payment Analysis (11)**
    - payment_method analytics
    - success rates
    - refund rates

14. **Marketing Campaigns (7)**
    - campaign_performance_summary
    - campaign metrics
    - device_conversion_rates

15. **Referrer & Channel (2)**
    - referrer_source_summary
    - referrer_churn_summary

16. **Conversion Funnel (6)**
    - high_value_funnel
    - funnel_summary
    - funnel by device/referrer

17. **Cart Behavior (7)**
    - cart_behavior_summary
    - high_value_abandoners
    - cart statistics

18. **Time to Purchase (3)**
    - time_to_purchase overall, by_tier, buckets

19. **Wishlist (9)**
    - wishlist_overall_summary
    - wishlist by product/customer
    - abandoned wishlist items

20. **Review Analysis (5)**
    - review_velocity (daily, weekly, monthly)
    - sentiment_by_category
    - product_monthly_rating_trends

21. **Rating Analysis (4)**
    - low_rated products
    - rating_tier analytics

22. **Operations & Fulfillment (16)**
    - processing by category/time
    - delivery days by location
    - shipping efficiency

23. **Inventory Carrying Cost (1)**
    - inventory_carrying_cost_overall

### Product Analysis (46 items)

24. **Product Performance (5)**
    - best_selling_products
    - highest_margin_products
    - product_performance_score

25. **Product Trends (5)**
    - category/product monthly trends
    - seasonality patterns

26. **Category Analysis (5)**
    - category_revenue_share
    - category metrics

27. **Seasonality (2)**
    - category seasonality
    - peak seasons

28. **Product Lifecycle (2)**
    - segments, summary

29. **Stock & Inventory (10)**
    - out_of_stock, stockout risk
    - dead stock, health metrics
    - overstock analysis

30. **Reorder Management (2)**
    - sku_reorder_urgency
    - reorder_point_breach_frequency

31. **Supplier-Product Relations (4)**
    - supplier_product_performance
    - stockout rates

32. **Product Discovery (6)**
    - category/product affinity
    - recommendations

33. **Product Views & Ratings (3)**
    - rating summaries
    - view-to-purchase

34. **Reserved Stock (1)**
    - reserved_vs_available

35. **Checkout Optimization (3)**
    - checkout_dropoff metrics

### Supplier Analysis (12 items)

36. **Supplier Performance (4)**
    - reliability
    - fulfillment performance
    - revenue contribution

37. **Supplier Inventory (2)**
    - stockouts by supplier

38. **Storage & Costs (4)**
    - storage cost efficiency
    - carrying costs
    - margin erosion

39. **Supplier Operations (2)**
    - days since restock
    - contract expiry

---

## Backend Implementation

### Analytics API Endpoints (COMPLETE ✅)

#### 1. Get All Analytics
```http
GET /analytics/data/{business_id}
GET /analytics/data/{business_id}?categories=customer_acquisition,revenue_analysis
```

Response:
```json
{
  "business_id": "business_123",
  "categories": {
    "customer_acquisition": {
      "category": "customer_acquisition",
      "analytics": {
        "new_customers_daily": {
          "data": [...],
          "columns": ["date", "new_customers", "total_customers"],
          "row_count": 365
        }
      },
      "total_count": 12
    }
  },
  "total_categories": 42,
  "total_analytics": 188
}
```

#### 2. Get Category Analytics
```http
GET /analytics/data/{business_id}/category/customer_acquisition
```

#### 3. Get Single File
```http
GET /analytics/data/{business_id}/file/business_health_daily
```

#### 4. List Available
```http
GET /analytics/list/{business_id}
```

#### 5. Get Categories
```http
GET /analytics/categories
```

#### 6. Clear Cache
```http
POST /analytics/clear-cache/{business_id}
```

### Re-Inference API (TODO 🔄)

#### Step 1: Update `machine-learning/specific/infer.py`

```python
def main(BUCKET_NAME, skip_training=False):
    """
    Run specific model inference with optional training.
    
    Args:
        BUCKET_NAME: MinIO bucket name
        skip_training: If True, skip training (for streaming mode)
    """
    if not skip_training:
        print("Training specific models...")
        train_all(BUCKET_NAME)
    else:
        print("Skipping training (streaming mode)")
    
    print("Running specific model inference...")
    
    # Classification models
    fulfillment_risk(BUCKET_NAME)
    product_bundling(BUCKET_NAME)
    
    # Regression models
    campaign_roi(BUCKET_NAME)
    delivery_time(BUCKET_NAME)
    demand_forecast(BUCKET_NAME)
    price_optimization(BUCKET_NAME)
    revenue_forecast(BUCKET_NAME)
    seasonal_trends(BUCKET_NAME)
    
    # Clustering models
    product_affinity(BUCKET_NAME)
    product_lifecycle(BUCKET_NAME)
    
    print("Specific model inference complete!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", default="pulse-bucket-1")
    parser.add_argument("--skip-training", action="store_true")
    args = parser.parse_args()
    
    main(args.bucket_name, args.skip_training)
```

#### Step 2: Create `api/services/reinference_service.py`

```python
"""
Re-Inference Service for running analysis and specific model training/inference.

This runs when user clicks "Re-Infer Forecasts" button.
"""

import os
import subprocess
import uuid
import asyncio
from datetime import datetime
from sqlalchemy import text


class ReInferenceService:
    """Service for re-running analysis and specific model inference."""
    
    def __init__(self, db, websocket_manager=None):
        self.db = db
        self.websocket_manager = websocket_manager
    
    async def start_reinference(self, business_id: str, user_id: str):
        """
        Start re-inference process: analysis.py + specific/infer.py
        
        Args:
            business_id: Business ID (bucket name)
            user_id: User ID
            
        Returns:
            pipeline_id: UUID for tracking
        """
        pipeline_id = str(uuid.uuid4())
        
        # Create pipeline status entry
        self.db.execute(
            text("""
                INSERT INTO pipeline_status 
                (pipeline_id, business_id, user_id, status, started_at, progress, current_step)
                VALUES (:pipeline_id, :business_id, :user_id, 'running', :started_at, 0, :step)
            """),
            {
                "pipeline_id": pipeline_id,
                "business_id": business_id,
                "user_id": user_id,
                "started_at": datetime.now(),
                "step": "Initializing re-inference"
            }
        )
        self.db.commit()
        
        # Start async task
        asyncio.create_task(
            self._run_reinference(pipeline_id, business_id, user_id)
        )
        
        return pipeline_id
    
    async def _run_reinference(self, pipeline_id: str, business_id: str, user_id: str):
        """
        Run re-inference steps with progress tracking.
        """
        try:
            # Step 1: Run analysis.py (0-50%)
            await self._update_progress(pipeline_id, 0, "Running analysis...")
            
            analysis_cmd = [
                "python", "analysis/analysis.py",
                "--bucket-name", business_id
            ]
            
            process = subprocess.Popen(
                analysis_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for analysis to complete
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"Analysis failed: {stderr}")
            
            await self._update_progress(pipeline_id, 50, "Analysis complete")
            
            # Step 2: Run specific/infer.py with training (50-100%)
            await self._update_progress(pipeline_id, 50, "Training specific models...")
            
            infer_cmd = [
                "python", "machine-learning/specific/infer.py",
                "--bucket-name", business_id
            ]
            
            process = subprocess.Popen(
                infer_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for inference to complete
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"Inference failed: {stderr}")
            
            await self._update_progress(pipeline_id, 100, "Re-inference complete!")
            
            # Mark as completed
            self.db.execute(
                text("""
                    UPDATE pipeline_status 
                    SET status = 'completed', completed_at = :completed_at, progress = 100
                    WHERE pipeline_id = :pipeline_id
                """),
                {
                    "pipeline_id": pipeline_id,
                    "completed_at": datetime.now()
                }
            )
            self.db.commit()
            
        except Exception as e:
            print(f"Re-inference error: {e}")
            
            # Mark as failed
            self.db.execute(
                text("""
                    UPDATE pipeline_status 
                    SET status = 'failed', error = :error
                    WHERE pipeline_id = :pipeline_id
                """),
                {
                    "pipeline_id": pipeline_id,
                    "error": str(e)
                }
            )
            self.db.commit()
            
            if self.websocket_manager:
                await self.websocket_manager.broadcast(
                    business_id,
                    {
                        "pipeline_id": pipeline_id,
                        "status": "failed",
                        "error": str(e)
                    }
                )
    
    async def _update_progress(self, pipeline_id: str, progress: int, step: str):
        """Update progress in database and broadcast via WebSocket."""
        self.db.execute(
            text("""
                UPDATE pipeline_status 
                SET progress = :progress, current_step = :step
                WHERE pipeline_id = :pipeline_id
            """),
            {
                "pipeline_id": pipeline_id,
                "progress": progress,
                "step": step
            }
        )
        self.db.commit()
        
        # Get business_id for WebSocket
        result = self.db.execute(
            text("SELECT business_id, status FROM pipeline_status WHERE pipeline_id = :pipeline_id"),
            {"pipeline_id": pipeline_id}
        ).fetchone()
        
        if result and self.websocket_manager:
            business_id, status = result
            await self.websocket_manager.broadcast(
                business_id,
                {
                    "pipeline_id": pipeline_id,
                    "status": status,
                    "progress": progress,
                    "current_step": step
                }
            )
```

#### Step 3: Add endpoint to `api/routers/pipeline.py`

```python
from services.reinference_service import ReInferenceService

@router.post("/re-infer-forecasts")
async def re_infer_forecasts(request: Request, db=Depends(get_db)):
    """
    Re-run analysis and specific model inference with training.
    
    This endpoint:
    1. Runs analysis.py
    2. Runs specific/infer.py with training
    3. Updates progress via WebSocket
    
    Request body:
        - userId: User ID
        - businessId: Business ID
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        business_id = body.get("businessId")
        
        if not user_id or not business_id:
            raise HTTPException(status_code=400, detail="userId and businessId are required")
        
        # Verify business belongs to user
        result = db.execute(
            text("SELECT business_id FROM businesses WHERE business_id = :business_id AND user_id = :user_id"),
            {"business_id": business_id, "user_id": user_id}
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Business not found or access denied")
        
        # Start re-inference
        reinference_service = ReInferenceService(db, websocket_manager)
        pipeline_id = await reinference_service.start_reinference(business_id, user_id)
        
        return {
            "status": 200,
            "message": "Re-inference started successfully",
            "pipeline_id": pipeline_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error starting re-inference: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
```

#### Step 4: Update streaming_ml_inference.py to skip specific models

In `machine-learning/streaming_ml_inference.py`, ensure only general models run:

```python
# In create_all_ml_inference_streams()
# Only create streams for general models
# Specific models require training, so skip them in streaming mode
```

---

## Frontend Implementation

### Installation

```bash
cd frontend
npm install react-chartjs-2 chart.js
```

### File Structure

```
frontend/src/
├── pages/
│   └── dashboard/
│       ├── analytics/
│       │   ├── AnalyticsDashboard.jsx       # Main container
│       │   ├── sections/
│       │   │   ├── KPISection.jsx
│       │   │   ├── CustomerSection.jsx
│       │   │   ├── ProductSection.jsx
│       │   │   ├── SupplierSection.jsx
│       │   │   ├── MarketingSection.jsx
│       │   │   ├── ConversionSection.jsx
│       │   │   ├── CartSection.jsx
│       │   │   ├── PaymentSection.jsx
│       │   │   ├── OperationsSection.jsx
│       │   │   ├── TimeSection.jsx
│       │   │   └── ReviewSection.jsx
│       │   └── components/
│       │       ├── AnalyticsCard.jsx
│       │       ├── ChartWrapper.jsx
│       │       ├── TrendChart.jsx
│       │       ├── BarChart.jsx
│       │       ├── PieChart.jsx
│       │       ├── HeatmapChart.jsx
│       │       ├── FunnelChart.jsx
│       │       ├── DataTable.jsx
│       │       ├── KPICard.jsx
│       │       ├── Loader.jsx
│       │       └── ErrorBoundary.jsx
│       └── components/
│           └── ReInferenceButton.jsx         # Re-inference button
└── contexts/
    └── AnalyticsContext.jsx                  # Analytics state management
```

### 1. Re-Inference Button Component

Create `frontend/src/pages/dashboard/components/ReInferenceButton.jsx`:

```jsx
import React, { useState } from 'react';
import { Button } from 'primereact/button';
import { ProgressBar } from 'primereact/progressbar';
import { Toast } from 'primereact/toast';
import { useAuth } from '../../../contexts/AuthContext';

const ReInferenceButton = ({ businessId }) => {
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const { user } = useAuth();
  const toast = React.useRef(null);
  
  const handleReInference = async () => {
    try {
      setLoading(true);
      setProgress(0);
      
      // Start re-inference
      const response = await fetch('/api/pipeline/re-infer-forecasts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: user.id,
          businessId: businessId
        })
      });
      
      if (!response.ok) {
        throw new Error('Failed to start re-inference');
      }
      
      const data = await response.json();
      const pipelineId = data.pipeline_id;
      
      // Connect to WebSocket for progress updates
      const ws = new WebSocket(`ws://localhost:8000/pipeline/ws/${businessId}`);
      
      ws.onmessage = (event) => {
        const update = JSON.parse(event.data);
        
        if (update.pipeline_id === pipelineId) {
          setProgress(update.progress);
          
          if (update.status === 'completed') {
            setLoading(false);
            toast.current.show({
              severity: 'success',
              summary: 'Success',
              detail: 'Forecasts re-inferred successfully!',
              life: 3000
            });
            ws.close();
          } else if (update.status === 'failed') {
            setLoading(false);
            toast.current.show({
              severity: 'error',
              summary: 'Error',
              detail: 'Re-inference failed',
              life: 3000
            });
            ws.close();
          }
        }
      };
      
      ws.onerror = () => {
        setLoading(false);
        toast.current.show({
          severity: 'error',
          summary: 'Error',
          detail: 'WebSocket connection failed',
          life: 3000
        });
      };
      
    } catch (error) {
      setLoading(false);
      toast.current.show({
        severity: 'error',
        summary: 'Error',
        detail: error.message,
        life: 3000
      });
    }
  };
  
  return (
    <>
      <Toast ref={toast} />
      <div className="re-inference-container">
        <Button
          label="Re-Infer Forecasts"
          icon="pi pi-refresh"
          onClick={handleReInference}
          loading={loading}
          className="p-button-primary"
          disabled={loading}
        />
        {loading && (
          <div className="mt-2">
            <ProgressBar value={progress} />
            <small className="text-muted">
              {progress < 50 ? 'Running analysis...' : 'Training models...'}
            </small>
          </div>
        )}
      </div>
    </>
  );
};

export default ReInferenceButton;
```

### 2. Analytics Dashboard Main Component

Create `frontend/src/pages/dashboard/analytics/AnalyticsDashboard.jsx`:

```jsx
import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { TabView, TabPanel } from 'primereact/tabview';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Message } from 'primereact/message';

import KPISection from './sections/KPISection';
import CustomerSection from './sections/CustomerSection';
import ProductSection from './sections/ProductSection';
import SupplierSection from './sections/SupplierSection';
import MarketingSection from './sections/MarketingSection';
import ConversionSection from './sections/ConversionSection';
import CartSection from './sections/CartSection';
import PaymentSection from './sections/PaymentSection';
import OperationsSection from './sections/OperationsSection';
import TimeSection from './sections/TimeSection';
import ReviewSection from './sections/ReviewSection';

const AnalyticsDashboard = ({ businessId }) => {
  const [loading, setLoading] = useState(false);
  const [analytics, setAnalytics] = useState({});
  const [error, setError] = useState(null);
  const [loadedCategories, setLoadedCategories] = useState([]);
  const location = useLocation();
  
  useEffect(() => {
    // Auto-fetch if navigated from pipeline completion
    if (location.state?.autoFetch) {
      loadAnalytics();
    }
  }, [location.state]);
  
  const loadAnalytics = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Fetch all analytics
      const response = await fetch(`/api/analytics/data/${businessId}`);
      
      if (!response.ok) {
        throw new Error('Failed to fetch analytics');
      }
      
      const data = await response.json();
      setAnalytics(data.categories);
      setLoadedCategories(Object.keys(data.categories));
      
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Progressive loading: load categories one by one
  const loadCategoryProgressively = async (category) => {
    try {
      const response = await fetch(
        `/api/analytics/data/${businessId}/category/${category}`
      );
      
      if (response.ok) {
        const data = await response.json();
        setAnalytics(prev => ({
          ...prev,
          [category]: data
        }));
        setLoadedCategories(prev => [...prev, category]);
      }
    } catch (err) {
      console.error(`Error loading ${category}:`, err);
    }
  };
  
  if (loading) {
    return (
      <div className="flex justify-content-center align-items-center" style={{height: '400px'}}>
        <div className="text-center">
          <ProgressSpinner />
          <p className="mt-3">Loading analytics data...</p>
          {loadedCategories.length > 0 && (
            <small className="text-muted">
              Loaded {loadedCategories.length} categories...
            </small>
          )}
        </div>
      </div>
    );
  }
  
  if (error) {
    return (
      <Message 
        severity="error" 
        text={error} 
        className="w-full"
      />
    );
  }
  
  return (
    <div className="analytics-dashboard">
      <div className="flex justify-content-between align-items-center mb-4">
        <h2>Analytics Dashboard</h2>
        <Button 
          label="Refresh Analytics" 
          icon="pi pi-refresh"
          onClick={loadAnalytics}
          className="p-button-outlined"
        />
      </div>
      
      <TabView>
        <TabPanel header="KPIs" leftIcon="pi pi-chart-line">
          <KPISection data={analytics.business_health} />
        </TabPanel>
        
        <TabPanel header="Customers" leftIcon="pi pi-users">
          <CustomerSection 
            data={{
              acquisition: analytics.customer_acquisition,
              demographics: analytics.customer_demographics,
              segmentation: analytics.customer_segmentation,
              engagement: analytics.customer_engagement,
              value: analytics.customer_value,
              churn: analytics.churn_risk
            }}
          />
        </TabPanel>
        
        <TabPanel header="Products" leftIcon="pi pi-box">
          <ProductSection 
            data={{
              performance: analytics.product_performance,
              trends: analytics.product_trends,
              inventory: analytics.stock_inventory
            }}
          />
        </TabPanel>
        
        <TabPanel header="Suppliers" leftIcon="pi pi-truck">
          <SupplierSection data={analytics.supplier_performance} />
        </TabPanel>
        
        <TabPanel header="Marketing" leftIcon="pi pi-megaphone">
          <MarketingSection data={analytics.marketing_campaigns} />
        </TabPanel>
        
        <TabPanel header="Conversion" leftIcon="pi pi-filter">
          <ConversionSection data={analytics.conversion_funnel} />
        </TabPanel>
        
        <TabPanel header="Cart & Checkout" leftIcon="pi pi-shopping-cart">
          <CartSection data={analytics.cart_behavior} />
        </TabPanel>
        
        <TabPanel header="Payments" leftIcon="pi pi-credit-card">
          <PaymentSection data={analytics.payment_analysis} />
        </TabPanel>
        
        <TabPanel header="Operations" leftIcon="pi pi-cog">
          <OperationsSection data={analytics.operations_fulfillment} />
        </TabPanel>
        
        <TabPanel header="Reviews" leftIcon="pi pi-star">
          <ReviewSection data={analytics.review_analysis} />
        </TabPanel>
      </TabView>
    </div>
  );
};

export default AnalyticsDashboard;
```

### 3. Example Section Component (KPI Section)

Create `frontend/src/pages/dashboard/analytics/sections/KPISection.jsx`:

```jsx
import React from 'react';
import { Card } from 'primereact/card';
import { Chart } from 'primereact/chart';
import KPICard from '../components/KPICard';
import TrendChart from '../components/TrendChart';

const KPISection = ({ data }) => {
  if (!data) {
    return <div>Loading KPI data...</div>;
  }
  
  const { analytics } = data;
  
  // Extract business health data
  const dailyHealth = analytics?.business_health_daily?.data || [];
  const weeklyHealth = analytics?.business_health_weekly?.data || [];
  const monthlyHealth = analytics?.business_health_monthly?.data || [];
  const lowMarginCategories = analytics?.low_margin_categories?.data || [];
  
  // Get latest metrics
  const latest = dailyHealth[dailyHealth.length - 1] || {};
  
  return (
    <div className="kpi-section">
      {/* Key Metrics Cards */}
      <div className="grid">
        <div className="col-12 md:col-6 lg:col-3">
          <KPICard
            title="Total Revenue"
            value={latest.total_revenue}
            change={latest.revenue_change_pct}
            icon="pi-dollar"
            color="blue"
          />
        </div>
        
        <div className="col-12 md:col-6 lg:col-3">
          <KPICard
            title="Total Orders"
            value={latest.total_orders}
            change={latest.orders_change_pct}
            icon="pi-shopping-cart"
            color="green"
          />
        </div>
        
        <div className="col-12 md:col-6 lg:col-3">
          <KPICard
            title="Average Order Value"
            value={latest.aov}
            change={latest.aov_change_pct}
            icon="pi-chart-line"
            color="orange"
          />
        </div>
        
        <div className="col-12 md:col-6 lg:col-3">
          <KPICard
            title="Conversion Rate"
            value={`${latest.conversion_rate}%`}
            change={latest.conversion_change_pct}
            icon="pi-percentage"
            color="purple"
          />
        </div>
      </div>
      
      {/* Trend Charts */}
      <div className="grid mt-4">
        <div className="col-12 lg:col-6">
          <Card title="Revenue Trend (Daily)">
            <TrendChart
              data={dailyHealth}
              xField="date"
              yField="total_revenue"
              label="Revenue"
            />
          </Card>
        </div>
        
        <div className="col-12 lg:col-6">
          <Card title="Orders Trend (Daily)">
            <TrendChart
              data={dailyHealth}
              xField="date"
              yField="total_orders"
              label="Orders"
            />
          </Card>
        </div>
      </div>
      
      {/* Low Margin Categories */}
      {lowMarginCategories.length > 0 && (
        <div className="mt-4">
          <Card title="Low Margin Categories">
            <DataTable 
              value={lowMarginCategories}
              paginator
              rows={10}
            >
              <Column field="category" header="Category" />
              <Column field="margin_pct" header="Margin %" />
              <Column field="revenue" header="Revenue" />
            </DataTable>
          </Card>
        </div>
      )}
    </div>
  );
};

export default KPISection;
```

### 4. Reusable Chart Components

Create `frontend/src/pages/dashboard/analytics/components/TrendChart.jsx`:

```jsx
import React from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

const TrendChart = ({ data, xField, yField, label }) => {
  const chartData = {
    labels: data.map(item => item[xField]),
    datasets: [{
      label: label,
      data: data.map(item => item[yField]),
      borderColor: 'rgb(75, 192, 192)',
      backgroundColor: 'rgba(75, 192, 192, 0.2)',
      tension: 0.1
    }]
  };
  
  const options = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: false
      }
    },
    scales: {
      y: {
        beginAtZero: true
      }
    }
  };
  
  return <Line data={chartData} options={options} />;
};

export default TrendChart;
```

Create `frontend/src/pages/dashboard/analytics/components/KPICard.jsx`:

```jsx
import React from 'react';
import { Card } from 'primereact/card';

const KPICard = ({ title, value, change, icon, color }) => {
  const isPositive = change >= 0;
  const changeColor = isPositive ? 'text-green-500' : 'text-red-500';
  const changeIcon = isPositive ? 'pi-arrow-up' : 'pi-arrow-down';
  
  return (
    <Card className={`kpi-card border-${color}-500`}>
      <div className="flex justify-content-between align-items-center">
        <div>
          <div className="text-500 mb-2">{title}</div>
          <div className="text-900 text-4xl font-bold">{value}</div>
          <div className={`${changeColor} mt-2`}>
            <i className={`pi ${changeIcon} mr-1`}></i>
            <span>{Math.abs(change)}%</span>
          </div>
        </div>
        <div className={`text-${color}-500`}>
          <i className={`pi ${icon} text-5xl`}></i>
        </div>
      </div>
    </Card>
  );
};

export default KPICard;
```

### 5. Auto-Navigate After Pipeline Completion

Update `frontend/src/contexts/PipelineProgressContext.jsx`:

```jsx
import { useNavigate } from 'react-router-dom';

const PipelineProgressContext = () => {
  const navigate = useNavigate();
  
  // ... existing code
  
  useEffect(() => {
    if (progress === 100 && status === 'completed') {
      // Show completion message
      toast.current.show({
        severity: 'success',
        summary: 'Pipeline Complete',
        detail: 'Loading analytics...',
        life: 2000
      });
      
      // Auto-navigate to analytics after 2 seconds
      setTimeout(() => {
        navigate('/dashboard/analytics', {
          state: {
            autoFetch: true,
            businessId: currentBusinessId,
            showWelcome: true
          }
        });
      }, 2000);
    }
  }, [progress, status, navigate]);
  
  // ... rest of code
};
```

---

## Testing Guide

### Backend Testing

```bash
# Test analytics API
curl http://localhost:8000/analytics/categories
curl http://localhost:8000/analytics/data/business_123
curl http://localhost:8000/analytics/data/business_123/category/customer_acquisition
curl http://localhost:8000/analytics/list/business_123

# Test re-inference
curl -X POST http://localhost:8000/pipeline/re-infer-forecasts \
  -H "Content-Type: application/json" \
  -d '{"userId":"user_123","businessId":"business_123"}'
```

### Frontend Testing

1. **Test Analytics Loading:**
   - Navigate to `/dashboard/analytics`
   - Verify progressive loading
   - Check all sections render

2. **Test Auto-Navigation:**
   - Start pipeline
   - Wait for 100% completion
   - Verify auto-redirect to analytics
   - Confirm data loads automatically

3. **Test Re-Inference:**
   - Click "Re-Infer Forecasts" button
   - Verify loading state shows
   - Check progress bar updates
   - Confirm completion notification

---

## Deployment Checklist

- [ ] Backend: Analytics service deployed
- [ ] Backend: Re-inference service deployed
- [ ] Database: Pipeline status table updated
- [ ] Frontend: Chart.js installed
- [ ] Frontend: Analytics pages deployed
- [ ] Frontend: Re-inference button added
- [ ] Frontend: Auto-navigation configured
- [ ] MinIO: Analytics bucket accessible
- [ ] WebSocket: Enabled and tested
- [ ] Error handling: All edge cases covered
- [ ] Performance: Caching tested
- [ ] UI/UX: Loading states polished

---

## Performance Optimization

### Backend
- Cache analytics for 5 minutes
- Use pagination for large datasets
- Stream large files instead of loading all
- Compress responses

### Frontend
- Lazy load sections
- Implement virtual scrolling for tables
- Use skeleton loaders
- Cache chart data locally
- Debounce filter changes

---

## Future Enhancements

1. **Real-time Updates:**
   - WebSocket for live metric updates
   - Auto-refresh every 30 seconds
   - New data notifications

2. **Advanced Filters:**
   - Date range selection
   - Category filters
   - Segment filters
   - Export to CSV/Excel

3. **Customization:**
   - Drag-and-drop dashboard layout
   - Save custom views
   - Create custom charts
   - Set alerts on thresholds

4. **Collaboration:**
   - Share dashboards
   - Add comments
   - Schedule reports
   - Email notifications

---

## Support

For issues or questions:
1. Check backend logs: `/var/log/api/`
2. Check MinIO connectivity
3. Verify parquet files exist
4. Test API endpoints directly
5. Review browser console for frontend errors

---

**Status:** Backend Phase 1-2 complete, Frontend implementation guide ready!
