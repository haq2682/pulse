import React, { useState, useEffect, useRef } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend } from 'chart.js';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';
import { usePipelineProgress } from '@/context/PipelineProgressContext';

// Register Chart.js components
ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend
);

const ExecutiveOverview = () => {
    const { businessId } = useParams();
    const toastRef = useRef(null);

    const { pipelineStatus } = usePipelineProgress();
    
    const [loading, setLoading] = useState(true);
    const [kpiData, setKpiData] = useState({});
    const [revenueData, setRevenueData] = useState([]);
    const [customerData, setCustomerData] = useState({});
    const [productData, setProductData] = useState([]);
    const [operationsData, setOperationsData] = useState({});
    const [marketingData, setMarketingData] = useState({});
    
    // Connect to WebSocket for real-time updates
    const { lastUpdate, isConnected } = useAnalyticsWebSocket(businessId);
    
    // Fetch analytics data
    useEffect(() => {
        if (businessId) {
            fetchExecutiveOverviewData();
        }
    }, [businessId]);
    
    // Handle real-time updates
    useEffect(() => {
        if (lastUpdate && lastUpdate.files) {
            console.log('Received analytics update:', lastUpdate);
            
            // Show notification
            if (toastRef.current) {
                toastRef.current.show({
                    severity: 'info',
                    summary: 'Data Updated',
                    detail: `${lastUpdate.total_files} metric(s) updated`,
                    life: 3000
                });
            }
            
            // Refresh data
            fetchExecutiveOverviewData();
        }
    }, [lastUpdate]);
    
    const fetchExecutiveOverviewData = async () => {
        if (!businessId) return;
        
        setLoading(true);
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            
            // Fetch key categories for executive overview
            const categories = ['kpis', 'customer_analytics', 'product_analytics', 'operations_analytics', 'marketing_analytics'];
            const categoriesParam = categories.join(',');
            
            const response = await fetch(`${apiUrl}/analytics/data/${businessId}?categories=${categoriesParam}`);
            
            if (response.ok) {
                const data = await response.json();
                console.log('Fetched analytics data:', data);
                
                // Process the data
                processAnalyticsData(data.categories || {});
            } else {
                console.error('Failed to fetch analytics');
                if (toastRef.current) {
                    toastRef.current.show({
                        severity: 'warn',
                        summary: 'No Data',
                        detail: 'Analytics data not available. Run the analytics pipeline first.',
                        life: 5000
                    });
                }
            }
        } catch (error) {
            console.error('Error fetching executive overview data:', error);
            if (toastRef.current) {
                toastRef.current.show({
                    severity: 'error',
                    summary: 'Error',
                    detail: 'Failed to load analytics data',
                    life: 5000
                });
            }
        } finally {
            setLoading(false);
        }
    };
    
    const processAnalyticsData = (categories) => {
        // Extract and process different analytics categories
        
        // KPIs - Business health metrics (includes revenue trend)
        if (categories.kpis) {
            const kpis = extractKPIs(categories.kpis);
            setKpiData(kpis);
            
            // Extract revenue trend from business_health_daily
            if (categories.kpis.business_health_daily && Array.isArray(categories.kpis.business_health_daily)) {
                const revenueTrend = categories.kpis.business_health_daily.map(item => ({
                    date: item.grain_date,
                    revenue: item.total_revenue || 0
                }));
                setRevenueData(revenueTrend);
            }
        }
        
        // Customer analytics
        if (categories.customer_analytics) {
            const customers = extractCustomerData(categories.customer_analytics);
            setCustomerData(customers);
        }
        
        // Product analytics
        if (categories.product_analytics) {
            const products = extractProductData(categories.product_analytics);
            setProductData(products);
        }
        
        // Operations analytics
        if (categories.operations_analytics) {
            const operations = extractOperationsData(categories.operations_analytics);
            setOperationsData(operations);
        }
        
        // Marketing analytics
        if (categories.marketing_analytics) {
            const marketing = extractMarketingData(categories.marketing_analytics);
            setMarketingData(marketing);
        }
    };
    
    const extractKPIs = (kpisCategory) => {
        // Extract key KPIs from the business health data
        const kpis = {
            totalRevenue: 0,
            totalOrders: 0,
            avgOrderValue: 0,
            totalCustomers: 0,
            profitMargin: 0,
            grossProfit: 0,
            netProfit: 0,
            avgCLV: 0,
            conversionRate: 0
        };
        
        // Process business_health_daily data if available (most recent data)
        if (kpisCategory.business_health_daily && Array.isArray(kpisCategory.business_health_daily)) {
            const latestData = kpisCategory.business_health_daily[kpisCategory.business_health_daily.length - 1] || {};
            kpis.totalRevenue = latestData.total_revenue || 0;
            kpis.totalOrders = latestData.total_orders || 0;
            kpis.avgOrderValue = latestData.aov || 0;
            kpis.profitMargin = latestData.margin_pct || 0;
            kpis.grossProfit = latestData.gross_profit || 0;
            kpis.netProfit = latestData.net_profit || 0;
        }
        
        // Get CLV from clv_summary
        if (kpisCategory.clv_summary && Array.isArray(kpisCategory.clv_summary)) {
            const clvData = kpisCategory.clv_summary[0] || {};
            kpis.avgCLV = clvData.avg_clv || 0;
            kpis.totalCustomers = clvData.customers || 0;
        }
        
        // Get conversion rate from funnel_summary
        if (kpisCategory.funnel_summary && Array.isArray(kpisCategory.funnel_summary)) {
            const funnelData = kpisCategory.funnel_summary[0] || {};
            kpis.conversionRate = funnelData.overall_conversion_rate || 0;
        }
        
        return kpis;
    };
    
    const extractRevenueData = (revenueCategory) => {
        // Extract revenue trend data from business_health_daily (in kpis category)
        // Since revenue is part of business health, we'll use that
        return [];
    };
    
    const extractCustomerData = (customerCategory) => {
        // Extract customer metrics
        const data = {
            totalCustomers: 0,
            newCustomers: 0,
            cumulativeCustomers: 0,
            newCustomersTrend: []
        };
        
        // Get latest new customers data
        if (customerCategory.new_customers_daily && Array.isArray(customerCategory.new_customers_daily)) {
            const recentData = customerCategory.new_customers_daily.slice(-30); // Last 30 days
            data.newCustomersTrend = recentData.map(item => ({
                date: item.grain_date,
                count: item.new_customers || 0
            }));
            
            // Get most recent new customers count
            if (recentData.length > 0) {
                data.newCustomers = recentData[recentData.length - 1].new_customers || 0;
            }
        }
        
        // Get cumulative customers (total)
        if (customerCategory.cumulative_customers_daily && Array.isArray(customerCategory.cumulative_customers_daily)) {
            const latest = customerCategory.cumulative_customers_daily[customerCategory.cumulative_customers_daily.length - 1];
            if (latest) {
                data.cumulativeCustomers = latest.cumulative_customers || 0;
                data.totalCustomers = latest.cumulative_customers || 0;
            }
        }
        
        return data;
    };
    
    const extractProductData = (productCategory) => {
        // Extract top products from best_selling_products
        const products = [];
        
        if (productCategory.best_selling_products && Array.isArray(productCategory.best_selling_products)) {
            return productCategory.best_selling_products.slice(0, 10).map(item => ({
                name: item.product_name || 'Unknown Product',
                sales: item.total_units_sold || 0,
                revenue: item.total_revenue || 0,
                category: item.category || ''
            }));
        }
        
        return products;
    };
    
    const extractOperationsData = (operationsCategory) => {
        // Extract operations metrics
        const data = {
            avgFulfillmentTime: 0,
            onTimeDeliveryRate: 0,
            processingTime: 0,
            deliveryDays: 0
        };
        
        // Process operations data based on actual schema
        // Look for delivery and processing metrics
        if (operationsCategory.processing_by_status && Array.isArray(operationsCategory.processing_by_status)) {
            // Aggregate processing data
            const processingData = operationsCategory.processing_by_status;
            if (processingData.length > 0) {
                const totalProcessing = processingData.reduce((sum, item) => sum + (item.avg_processing_duration_hours || 0), 0);
                data.processingTime = totalProcessing / processingData.length;
            }
        }
        
        // Look for delivery metrics
        if (operationsCategory.ontime_delivery_by_country && Array.isArray(operationsCategory.ontime_delivery_by_country)) {
            const deliveryData = operationsCategory.ontime_delivery_by_country;
            if (deliveryData.length > 0) {
                const totalOnTime = deliveryData.reduce((sum, item) => sum + (item.ontime_delivery_rate || 0), 0);
                data.onTimeDeliveryRate = totalOnTime / deliveryData.length;
            }
        }
        
        // Look for delivery days
        if (operationsCategory.delivery_days_by_country && Array.isArray(operationsCategory.delivery_days_by_country)) {
            const deliveryData = operationsCategory.delivery_days_by_country;
            if (deliveryData.length > 0) {
                const totalDays = deliveryData.reduce((sum, item) => sum + (item.avg_delivery_days || 0), 0);
                data.deliveryDays = totalDays / deliveryData.length;
            }
        }
        
        return data;
    };
    
    const extractMarketingData = (marketingCategory) => {
        // Extract marketing metrics
        const data = {
            totalCampaigns: 0,
            avgROI: 0,
            totalCampaignRevenue: 0,
            avgCampaignCost: 0
        };
        
        // Process campaign_performance_summary if available
        if (marketingCategory.campaign_performance_summary && Array.isArray(marketingCategory.campaign_performance_summary)) {
            const campaigns = marketingCategory.campaign_performance_summary;
            data.totalCampaigns = campaigns.length;
            
            if (campaigns.length > 0) {
                const totalROI = campaigns.reduce((sum, item) => sum + (item.campaign_roi || 0), 0);
                const totalRevenue = campaigns.reduce((sum, item) => sum + (item.total_revenue || 0), 0);
                const totalCost = campaigns.reduce((sum, item) => sum + (item.total_cost || 0), 0);
                
                data.avgROI = totalROI / campaigns.length;
                data.totalCampaignRevenue = totalRevenue;
                data.avgCampaignCost = totalCost / campaigns.length;
            }
        }
        
        return data;
    };
    
    // Format currency
    const formatCurrency = (value) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0
        }).format(value);
    };
    
    // Format number
    const formatNumber = (value) => {
        return new Intl.NumberFormat('en-US').format(value);
    };
    
    // Format percentage
    const formatPercentage = (value) => {
        return `${value?.toFixed(1)}%`;
    };
    
    // Revenue chart data
    const revenueChartData = {
        labels: revenueData.map(d => d.date),
        datasets: [
            {
                label: 'Revenue',
                data: revenueData.map(d => d.revenue),
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                tension: 0.4
            }
        ]
    };
    
    const revenueChartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'top'
            },
            title: {
                display: true,
                text: 'Revenue Trend'
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: {
                    callback: function(value) {
                        return '$' + value.toLocaleString();
                    }
                }
            }
        }
    };
    
    // Product chart data
    const productChartData = {
        labels: productData.slice(0, 5).map(p => p.name),
        datasets: [
            {
                label: 'Sales',
                data: productData.slice(0, 5).map(p => p.sales),
                backgroundColor: [
                    'rgba(255, 99, 132, 0.6)',
                    'rgba(54, 162, 235, 0.6)',
                    'rgba(255, 206, 86, 0.6)',
                    'rgba(75, 192, 192, 0.6)',
                    'rgba(153, 102, 255, 0.6)'
                ]
            }
        ]
    };
    
    const productChartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'right'
            },
            title: {
                display: true,
                text: 'Top 5 Products'
            }
        }
    };
    
    if (loading && pipelineStatus !== 'loading') {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading executive overview...</p>
            </div>
        );
    }
    
    // Check if we have any data to display
    const hasAnyData = () => {
        const hasKPIs = kpiData.totalRevenue > 0 || kpiData.totalOrders > 0 || kpiData.avgOrderValue > 0 || 
                       kpiData.totalCustomers > 0 || kpiData.profitMargin > 0 || kpiData.avgCLV > 0 ||
                       kpiData.conversionRate > 0;
        const hasRevenue = revenueData.length > 0;
        const hasProducts = productData.length > 0;
        const hasCustomerMetrics = customerData.totalCustomers > 0 || customerData.newCustomers > 0 || 
                                   customerData.cumulativeCustomers > 0;
        const hasOperationsMetrics = operationsData.avgFulfillmentTime > 0 || operationsData.onTimeDeliveryRate > 0 || 
                                     operationsData.processingTime > 0 || operationsData.deliveryDays > 0;
        const hasMarketingMetrics = marketingData.totalCampaigns > 0 || marketingData.avgROI > 0 || 
                                   marketingData.totalCampaignRevenue > 0;
        
        return hasKPIs || hasRevenue || hasProducts || hasCustomerMetrics || hasOperationsMetrics || hasMarketingMetrics;
    };
    
    // If no data at all, show message
    if (!hasAnyData() && pipelineStatus !== 'loading') {
        return (
            <div className="p-6 min-h-[calc(100vh-120px)]">
                <Toast ref={toastRef} />
                <div className="flex items-center justify-center min-h-[60vh]">
                    <p className="text-gray-500 text-lg">No data to display</p>
                </div>
                {/* Connection Status */}
                {isConnected && (
                    <div className="fixed bottom-8 right-8 flex items-center gap-2 px-5 py-3 bg-white border border-green-500 rounded-full shadow-lg z-50">
                        <i className="pi pi-circle-fill text-[0.625rem] text-green-500 animate-pulse"></i>
                        <span className="text-sm font-semibold text-green-500">Live Updates Active</span>
                    </div>
                )}
            </div>
        );
    }
    
    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />
            
            {/* KPI Cards - Only show if we have data */}
            {(kpiData.totalRevenue > 0 || kpiData.totalOrders > 0 || kpiData.avgOrderValue > 0 || 
              kpiData.totalCustomers > 0 || kpiData.profitMargin > 0 || kpiData.avgCLV > 0 || 
              kpiData.conversionRate > 0) && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {/* Only show KPI cards that have data */}
                {kpiData.totalRevenue > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-dollar text-4xl p-4 bg-green-50 text-green-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatCurrency(kpiData.totalRevenue)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Revenue</p>
                            </div>
                        </div>
                    </Card>
                )}
                
                {kpiData.totalOrders > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-shopping-cart text-4xl p-4 bg-blue-50 text-blue-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatNumber(kpiData.totalOrders)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Orders</p>
                            </div>
                        </div>
                    </Card>
                )}
                
                {kpiData.avgOrderValue > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-chart-line text-4xl p-4 bg-orange-50 text-orange-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatCurrency(kpiData.avgOrderValue)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Average Order Value</p>
                            </div>
                        </div>
                    </Card>
                )}
                
                {kpiData.totalCustomers > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-users text-4xl p-4 bg-purple-50 text-purple-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatNumber(kpiData.totalCustomers)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Customers</p>
                            </div>
                        </div>
                    </Card>
                )}
                
                {kpiData.profitMargin > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-percentage text-4xl p-4 bg-red-50 text-red-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatPercentage(kpiData.profitMargin)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Profit Margin</p>
                            </div>
                        </div>
                    </Card>
                )}
                
                {kpiData.avgCLV > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-star text-4xl p-4 bg-yellow-50 text-yellow-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatCurrency(kpiData.avgCLV)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Avg Customer Lifetime Value</p>
                            </div>
                        </div>
                    </Card>
                )}
                
                {kpiData.conversionRate > 0 && (
                    <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                        <div className="flex items-center gap-5 p-6">
                            <i className="pi pi-chart-bar text-4xl p-4 bg-cyan-50 text-cyan-500 rounded-xl"></i>
                            <div>
                                <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatPercentage(kpiData.conversionRate)}</h3>
                                <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Conversion Rate</p>
                            </div>
                        </div>
                    </Card>
                )}
            </div>
            )}
            
            {/* Charts Section */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
                {/* Revenue Trend Chart */}
                {revenueData.length > 0 && (
                    <div className="col-span-1 lg:col-span-2">
                        <ChartWrapper 
                            title="Revenue Trend"
                            showUpdateBadge={false}
                        >
                            <div className="h-[300px]">
                                <Line data={revenueChartData} options={revenueChartOptions} />
                            </div>
                        </ChartWrapper>
                    </div>
                )}
                
                {/* Top Products Chart */}
                {productData.length > 0 && (
                    <ChartWrapper 
                        title="Top Products"
                        showUpdateBadge={false}
                    >
                        <div className="h-[300px]">
                            <Doughnut data={productChartData} options={productChartOptions} />
                        </div>
                    </ChartWrapper>
                )}
                
                {/* Customer Metrics Card - Only show if we have customer data */}
                {(customerData.totalCustomers > 0 || customerData.newCustomers > 0 || 
                  customerData.cumulativeCustomers > 0) && (
                    <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">Customer Metrics</h3>
                            <div className="flex flex-col gap-4">
                                {customerData.totalCustomers > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Total Customers</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatNumber(customerData.totalCustomers)}</span>
                                    </div>
                                )}
                                {customerData.newCustomers > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">New Customers (Recent)</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatNumber(customerData.newCustomers)}</span>
                                    </div>
                                )}
                                {customerData.cumulativeCustomers > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Cumulative Customers</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatNumber(customerData.cumulativeCustomers)}</span>
                                    </div>
                                )}
                            </div>
                        </div>
                    </Card>
                )}
                
                {/* Operations Metrics Card - Only show if we have operations data */}
                {(operationsData.processingTime > 0 || operationsData.onTimeDeliveryRate > 0 || 
                  operationsData.deliveryDays > 0) && (
                    <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">Operations Metrics</h3>
                            <div className="flex flex-col gap-4">
                                {operationsData.processingTime > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Avg Processing Time</span>
                                        <span className="text-gray-900 font-semibold text-lg">{operationsData.processingTime?.toFixed(1) || '0'} hrs</span>
                                    </div>
                                )}
                                {operationsData.onTimeDeliveryRate > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">On-Time Delivery</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatPercentage(operationsData.onTimeDeliveryRate)}</span>
                                    </div>
                                )}
                                {operationsData.deliveryDays > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Avg Delivery Days</span>
                                        <span className="text-gray-900 font-semibold text-lg">{operationsData.deliveryDays?.toFixed(1) || '0'} days</span>
                                    </div>
                                )}
                            </div>
                        </div>
                    </Card>
                )}
                
                {/* Marketing Metrics Card - Only show if we have marketing data */}
                {(marketingData.totalCampaigns > 0 || marketingData.avgROI > 0 || 
                  marketingData.totalCampaignRevenue > 0) && (
                    <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
                        <div className="p-6">
                            <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">Marketing Performance</h3>
                            <div className="flex flex-col gap-4">
                                {marketingData.totalCampaigns > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Total Campaigns</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatNumber(marketingData.totalCampaigns)}</span>
                                    </div>
                                )}
                                {marketingData.avgROI > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Average ROI</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatPercentage(marketingData.avgROI)}</span>
                                    </div>
                                )}
                                {marketingData.totalCampaignRevenue > 0 && (
                                    <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                        <span className="text-gray-700 font-medium">Campaign Revenue</span>
                                        <span className="text-gray-900 font-semibold text-lg">{formatCurrency(marketingData.totalCampaignRevenue)}</span>
                                    </div>
                                )}
                            </div>
                        </div>
                    </Card>
                )}
            </div>
            
            {/* Connection Status */}
            {isConnected && (
                <div className="fixed bottom-8 right-8 flex items-center gap-2 px-5 py-3 bg-white border border-green-500 rounded-full shadow-lg z-50">
                    <i className="pi pi-circle-fill text-[0.625rem] text-green-500 animate-pulse"></i>
                    <span className="text-sm font-semibold text-green-500">Live Updates Active</span>
                </div>
            )}
        </div>
    );
};

export default ExecutiveOverview;
