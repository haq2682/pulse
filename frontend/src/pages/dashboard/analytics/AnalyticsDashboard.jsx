import React, { useState, useEffect } from 'react';
import { useParams, useLocation } from 'react-router-dom';
import { useAnalyticsWebSocket } from '../../../hooks/useAnalyticsWebSocket';
import { Toast } from 'primereact/toast';
import { Badge } from 'primereact/badge';
import { Button } from 'primereact/button';
import { ProgressSpinner } from 'primereact/progressspinner';
import './analytics.css';

const AnalyticsDashboard = () => {
    const { businessId } = useParams();
    const location = useLocation();
    const toastRef = React.useRef(null);
    
    const [loading, setLoading] = useState(false);
    const [analyticsData, setAnalyticsData] = useState({});
    const [loadedCategories, setLoadedCategories] = useState([]);
    
    // Connect to analytics WebSocket
    const { 
        updates, 
        isConnected, 
        lastUpdate, 
        triggerRefresh,
        clearUpdates 
    } = useAnalyticsWebSocket(businessId);
    
    // Auto-fetch analytics if coming from pipeline completion
    useEffect(() => {
        const { autoFetch } = location.state || {};
        
        if (autoFetch && businessId) {
            console.log('Auto-fetching analytics after pipeline completion');
            fetchAnalytics();
        }
    }, [location.state, businessId]);
    
    // Handle real-time updates
    useEffect(() => {
        if (lastUpdate && lastUpdate.files) {
            console.log('Analytics update received:', lastUpdate);
            
            // Show notification
            if (toastRef.current) {
                toastRef.current.show({
                    severity: 'info',
                    summary: 'Analytics Updated',
                    detail: `${lastUpdate.total_files} chart${lastUpdate.total_files > 1 ? 's' : ''} updated`,
                    life: 3000
                });
            }
            
            // Refresh affected analytics
            refreshAnalytics(lastUpdate.files);
        }
    }, [lastUpdate]);
    
    // Fetch analytics data from backend
    const fetchAnalytics = async () => {
        if (!businessId) return;
        
        setLoading(true);
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/analytics/data/${businessId}`);
            
            if (response.ok) {
                const data = await response.json();
                setAnalyticsData(data.categories || {});
                setLoadedCategories(Object.keys(data.categories || {}));
            } else {
                console.error('Failed to fetch analytics');
                if (toastRef.current) {
                    toastRef.current.show({
                        severity: 'error',
                        summary: 'Error',
                        detail: 'Failed to load analytics data',
                        life: 5000
                    });
                }
            }
        } catch (error) {
            console.error('Error fetching analytics:', error);
            if (toastRef.current) {
                toastRef.current.show({
                    severity: 'error',
                    summary: 'Error',
                    detail: 'Failed to connect to analytics service',
                    life: 5000
                });
            }
        } finally {
            setLoading(false);
        }
    };
    
    // Refresh specific analytics files
    const refreshAnalytics = async (files) => {
        if (!businessId || !files || files.length === 0) return;
        
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            
            // Fetch updated data for each file
            for (const fileName of files) {
                const response = await fetch(`${apiUrl}/analytics/data/${businessId}/file/${fileName}`);
                
                if (response.ok) {
                    const data = await response.json();
                    
                    // Update analytics data state
                    setAnalyticsData(prev => ({
                        ...prev,
                        [fileName]: data
                    }));
                }
            }
        } catch (error) {
            console.error('Error refreshing analytics:', error);
        }
    };
    
    // Handle manual refresh button
    const handleManualRefresh = () => {
        triggerRefresh();
        fetchAnalytics();
    };
    
    return (
        <div className="analytics-dashboard">
            <Toast ref={toastRef} />
            
            {/* Header */}
            <div className="analytics-header">
                <div className="header-left">
                    <h1 className="dashboard-title">Analytics Dashboard</h1>
                    <Badge 
                        value={isConnected ? 'Live' : 'Offline'} 
                        severity={isConnected ? 'success' : 'danger'}
                        className="connection-badge"
                    />
                </div>
                
                <div className="header-right">
                    <Button
                        label="Refresh"
                        icon="pi pi-refresh"
                        onClick={handleManualRefresh}
                        className="p-button-outlined"
                        disabled={loading}
                    />
                </div>
            </div>
            
            {/* Loading State */}
            {loading && (
                <div className="loading-container">
                    <ProgressSpinner />
                    <p>Loading analytics data...</p>
                </div>
            )}
            
            {/* Content */}
            {!loading && (
                <div className="analytics-content">
                    {loadedCategories.length === 0 ? (
                        <div className="empty-state">
                            <i className="pi pi-chart-bar" style={{ fontSize: '4rem', color: '#ccc' }}></i>
                            <h3>No Analytics Available</h3>
                            <p>Run the analytics pipeline to generate insights.</p>
                            <Button
                                label="Load Analytics"
                                icon="pi pi-download"
                                onClick={fetchAnalytics}
                                className="p-button-primary"
                            />
                        </div>
                    ) : (
                        <div className="analytics-grid">
                            {/* Placeholder for actual charts */}
                            <div className="analytics-section">
                                <h2>Business Health</h2>
                                <div className="charts-container">
                                    <div className="chart-placeholder">
                                        <p>Charts will be displayed here</p>
                                        <p className="text-muted">
                                            {Object.keys(analyticsData).length} analytics loaded
                                        </p>
                                    </div>
                                </div>
                            </div>
                            
                            {/* Update indicator */}
                            {lastUpdate && (
                                <div className="update-indicator">
                                    <i className="pi pi-check-circle"></i>
                                    <span>Last updated: {new Date(lastUpdate.timestamp).toLocaleTimeString()}</span>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export default AnalyticsDashboard;
