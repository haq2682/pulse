import React, { useState, useEffect } from 'react';
import { Badge } from 'primereact/badge';
import { ProgressSpinner } from 'primereact/progressspinner';
import '../analytics.css';

/**
 * ChartWrapper component that handles loading states, updates, and displays charts
 * with smooth transitions and update indicators
 */
const ChartWrapper = ({ 
    title, 
    children, 
    isLoading = false, 
    lastUpdated = null,
    showUpdateBadge = false,
    onRefresh = null 
}) => {
    const [showBadge, setShowBadge] = useState(false);
    const [isUpdating, setIsUpdating] = useState(false);
    
    // Show update badge when data is refreshed
    useEffect(() => {
        if (showUpdateBadge) {
            setShowBadge(true);
            setIsUpdating(true);
            
            // Trigger pulse animation
            const timer = setTimeout(() => {
                setIsUpdating(false);
            }, 500);
            
            // Hide badge after 3 seconds
            const badgeTimer = setTimeout(() => {
                setShowBadge(false);
            }, 3000);
            
            return () => {
                clearTimeout(timer);
                clearTimeout(badgeTimer);
            };
        }
    }, [showUpdateBadge, lastUpdated]);
    
    return (
        <div className={`chart-wrapper ${isUpdating ? 'updating chart-updated' : ''}`}>
            {/* Title Bar */}
            <div className="chart-header">
                <h3 className="chart-title">{title}</h3>
                {lastUpdated && (
                    <span className="chart-timestamp">
                        <i className="pi pi-clock"></i>
                        {' '}
                        {new Date(lastUpdated).toLocaleTimeString()}
                    </span>
                )}
            </div>
            
            {/* Update Badge */}
            {showBadge && (
                <Badge 
                    value="Updated" 
                    severity="success" 
                    className="chart-badge"
                />
            )}
            
            {/* Loading State */}
            {isLoading && (
                <div className="chart-loading">
                    <ProgressSpinner style={{ width: '50px', height: '50px' }} />
                    <p>Loading chart data...</p>
                </div>
            )}
            
            {/* Chart Content */}
            {!isLoading && (
                <div className="chart-content">
                    {children}
                </div>
            )}
            
            {/* Refresh Button (Optional) */}
            {onRefresh && !isLoading && (
                <button 
                    className="chart-refresh-btn" 
                    onClick={onRefresh}
                    title="Refresh chart"
                >
                    <i className="pi pi-refresh"></i>
                </button>
            )}
        </div>
    );
};

export default ChartWrapper;
