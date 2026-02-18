import React, { useState, useEffect, useRef } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from 'primereact/card';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Toast } from 'primereact/toast';
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, BarElement, ArcElement, Title, Tooltip, Legend } from 'chart.js';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import { useAnalyticsWebSocket } from '../../../../hooks/useAnalyticsWebSocket';
import ChartWrapper from '../components/ChartWrapper';

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
            const categories = ['kpis', 'revenue_analytics', 'customer_analytics', 'product_analytics', 'operations_analytics', 'marketing_analytics'];
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
        
        // KPIs - Business health metrics
        if (categories.kpis) {
            const kpis = extractKPIs(categories.kpis);
            setKpiData(kpis);
        }
        
        // Revenue analytics
        if (categories.revenue_analytics) {
            const revenue = extractRevenueData(categories.revenue_analytics);
            setRevenueData(revenue);
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
            growthRate: 0
        };
        
        // Process business_health data if available
        if (kpisCategory.business_health_daily && kpisCategory.business_health_daily.data) {
            const latestData = kpisCategory.business_health_daily.data[0] || {};
            kpis.totalRevenue = latestData.total_revenue || 0;
            kpis.totalOrders = latestData.total_orders || 0;
            kpis.avgOrderValue = latestData.avg_order_value || 0;
            kpis.profitMargin = latestData.profit_margin || 0;
        }
        
        return kpis;
    };
    
    const extractRevenueData = (revenueCategory) => {
        // Extract revenue trend data
        const revenueData = [];
        
        // Look for daily/weekly/monthly revenue data
        // This would come from revenue analytics or business health
        if (revenueCategory.rev_by_date && revenueCategory.rev_by_date.data) {
            return revenueCategory.rev_by_date.data.map(item => ({
                date: item.date,
                revenue: item.revenue || item.total_revenue || 0
            }));
        }
        
        return revenueData;
    };
    
    const extractCustomerData = (customerCategory) => {
        // Extract customer metrics
        const data = {
            totalCustomers: 0,
            newCustomers: 0,
            returningCustomers: 0,
            churnRate: 0
        };
        
        // Process customer data
        if (customerCategory.customer_summary && customerCategory.customer_summary.data) {
            const summary = customerCategory.customer_summary.data[0] || {};
            data.totalCustomers = summary.total_customers || 0;
            data.newCustomers = summary.new_customers || 0;
            data.returningCustomers = summary.returning_customers || 0;
        }
        
        return data;
    };
    
    const extractProductData = (productCategory) => {
        // Extract top products
        const products = [];
        
        if (productCategory.best_selling_products && productCategory.best_selling_products.data) {
            return productCategory.best_selling_products.data.slice(0, 10).map(item => ({
                name: item.product_name || item.product,
                sales: item.total_sales || item.sales || 0,
                revenue: item.revenue || 0
            }));
        }
        
        return products;
    };
    
    const extractOperationsData = (operationsCategory) => {
        // Extract operations metrics
        const data = {
            avgFulfillmentTime: 0,
            onTimeDelivery: 0,
            inventoryHealth: 0
        };
        
        if (operationsCategory.operations_summary && operationsCategory.operations_summary.data) {
            const summary = operationsCategory.operations_summary.data[0] || {};
            data.avgFulfillmentTime = summary.avg_fulfillment_time || 0;
            data.onTimeDelivery = summary.on_time_delivery_rate || 0;
        }
        
        return data;
    };
    
    const extractMarketingData = (marketingCategory) => {
        // Extract marketing metrics
        const data = {
            totalCampaigns: 0,
            activeCampaigns: 0,
            avgROI: 0
        };
        
        if (marketingCategory.campaign_summary && marketingCategory.campaign_summary.data) {
            const summary = marketingCategory.campaign_summary.data[0] || {};
            data.totalCampaigns = summary.total_campaigns || 0;
            data.activeCampaigns = summary.active_campaigns || 0;
            data.avgROI = summary.avg_roi || 0;
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
    
    if (loading) {
        return (
            <div className="flex flex-col items-center justify-center min-h-[400px] gap-4">
                <ProgressSpinner />
                <p className="text-gray-500 text-base">Loading executive overview...</p>
            </div>
        );
    }
    
    return (
        <div className="p-6 bg-gray-50 min-h-[calc(100vh-120px)]">
            <Toast ref={toastRef} />
            
            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                    <div className="flex items-center gap-5 p-6">
                        <i className="pi pi-dollar text-4xl p-4 bg-green-50 text-green-500 rounded-xl"></i>
                        <div>
                            <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatCurrency(kpiData.totalRevenue)}</h3>
                            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Revenue</p>
                        </div>
                    </div>
                </Card>
                
                <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                    <div className="flex items-center gap-5 p-6">
                        <i className="pi pi-shopping-cart text-4xl p-4 bg-blue-50 text-blue-500 rounded-xl"></i>
                        <div>
                            <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatNumber(kpiData.totalOrders)}</h3>
                            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Orders</p>
                        </div>
                    </div>
                </Card>
                
                <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                    <div className="flex items-center gap-5 p-6">
                        <i className="pi pi-chart-line text-4xl p-4 bg-orange-50 text-orange-500 rounded-xl"></i>
                        <div>
                            <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatCurrency(kpiData.avgOrderValue)}</h3>
                            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Average Order Value</p>
                        </div>
                    </div>
                </Card>
                
                <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                    <div className="flex items-center gap-5 p-6">
                        <i className="pi pi-users text-4xl p-4 bg-purple-50 text-purple-500 rounded-xl"></i>
                        <div>
                            <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatNumber(customerData.totalCustomers)}</h3>
                            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Total Customers</p>
                        </div>
                    </div>
                </Card>
                
                <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                    <div className="flex items-center gap-5 p-6">
                        <i className="pi pi-percentage text-4xl p-4 bg-red-50 text-red-500 rounded-xl"></i>
                        <div>
                            <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatPercentage(kpiData.profitMargin)}</h3>
                            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Profit Margin</p>
                        </div>
                    </div>
                </Card>
                
                <Card className="bg-gradient-to-br from-white to-gray-50 border border-gray-200 rounded-xl p-0 shadow-sm hover:shadow-md hover:-translate-y-1 transition-all duration-300">
                    <div className="flex items-center gap-5 p-6">
                        <i className="pi pi-chart-bar text-4xl p-4 bg-cyan-50 text-cyan-500 rounded-xl"></i>
                        <div>
                            <h3 className="text-2xl font-bold text-gray-900 mb-2">{formatPercentage(kpiData.growthRate)}</h3>
                            <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">Growth Rate</p>
                        </div>
                    </div>
                </Card>
            </div>
            
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
                
                {/* Customer Metrics Card */}
                <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">Customer Metrics</h3>
                        <div className="flex flex-col gap-4">
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Total Customers</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatNumber(customerData.totalCustomers)}</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">New Customers</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatNumber(customerData.newCustomers)}</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Returning Customers</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatNumber(customerData.returningCustomers)}</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Churn Rate</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatPercentage(customerData.churnRate)}</span>
                            </div>
                        </div>
                    </div>
                </Card>
                
                {/* Operations Metrics Card */}
                <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">Operations Metrics</h3>
                        <div className="flex flex-col gap-4">
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Avg Fulfillment Time</span>
                                <span className="text-gray-900 font-semibold text-lg">{operationsData.avgFulfillmentTime?.toFixed(1)} days</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">On-Time Delivery</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatPercentage(operationsData.onTimeDelivery)}</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Inventory Health</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatPercentage(operationsData.inventoryHealth)}</span>
                            </div>
                        </div>
                    </div>
                </Card>
                
                {/* Marketing Metrics Card */}
                <Card className="bg-white border border-gray-200 rounded-xl p-0 shadow-sm">
                    <div className="p-6">
                        <h3 className="text-xl font-semibold text-gray-900 mb-6 pb-3 border-b-2 border-gray-200">Marketing Performance</h3>
                        <div className="flex flex-col gap-4">
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Total Campaigns</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatNumber(marketingData.totalCampaigns)}</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Active Campaigns</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatNumber(marketingData.activeCampaigns)}</span>
                            </div>
                            <div className="flex justify-between items-center p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                                <span className="text-gray-700 font-medium">Average ROI</span>
                                <span className="text-gray-900 font-semibold text-lg">{formatPercentage(marketingData.avgROI)}</span>
                            </div>
                        </div>
                    </div>
                </Card>
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
