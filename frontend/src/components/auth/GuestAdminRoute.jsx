import React from 'react';
import { Navigate } from 'react-router';
import { useAdminAuth } from '@/context/AdminAuthContext';
import { ProgressSpinner } from 'primereact/progressspinner';

const GuestAdminRoute = ({ children }) => {
    const { isAuthenticated, loading } = useAdminAuth();

    if (loading) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-gray-50">
                <ProgressSpinner 
                    style={{ width: '50px', height: '50px' }} 
                    strokeWidth="4" 
                />
            </div>
        );
    }

    // If already logged in as Admin, go to Dashboard
    if (isAuthenticated) {
        return <Navigate to="/admin/dashboard" replace />;
    }

    return children;
};

export default GuestAdminRoute;