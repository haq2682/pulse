import React from 'react';
import { Navigate } from 'react-router';
import { useAuth } from '@/context/AuthContext';
import { ProgressSpinner } from 'primereact/progressspinner';

const GuestRoute = ({ children }) => {
    // FIX: Use 'appLoading' instead of 'loading'
    const { isAuthenticated, appLoading } = useAuth();

    if (appLoading) {
        return (
            <div className="min-h-screen flex items-center justify-center">
                <ProgressSpinner 
                    style={{ width: '50px', height: '50px' }} 
                    strokeWidth="4" 
                />
            </div>
        );
    }

    if (isAuthenticated) {
        return <Navigate to="/analytics" replace />;
    }

    return children;
};

export default GuestRoute;