import React from 'react';
import { Navigate, useLocation } from 'react-router';
import { useAdminAuth } from '@/context/AdminAuthContext';
import { ProgressSpinner } from 'primereact/progressspinner';

const ProtectedAdminRoute = ({ children }) => {
    // 1. Use the ADMIN auth hook, not the standard useAuth
    const { isAuthenticated, loading } = useAdminAuth();
    const location = useLocation();

    // 2. Show spinner while checking admin session
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

    // 3. If not an admin, redirect specifically to ADMIN LOGIN
    if (!isAuthenticated) {
        return <Navigate to="/admin/login" state={{ from: location }} replace />;
    }

    return children;
};

export default ProtectedAdminRoute;