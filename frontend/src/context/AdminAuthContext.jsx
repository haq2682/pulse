import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useNavigate, useLocation } from 'react-router';
import adminApi from '@/services/api/adminApi';

const AdminAuthContext = createContext(null);

export const AdminAuthProvider = ({ children }) => {
    const [admin, setAdmin] = useState(null);
    const [loading, setLoading] = useState(true); // App loading
    const [actionLoading, setActionLoading] = useState(false); // Button loading
    const [error, setError] = useState(null);
    
    const navigate = useNavigate();
    const location = useLocation();

    // Only check admin auth if we are actually ON an admin page
    useEffect(() => {
        if (location.pathname.startsWith('/admin')) {
            checkAdminAuth();
        } else {
            setLoading(false);
        }
    }, [location.pathname]);

    const checkAdminAuth = useCallback(async () => {
        try {
            // We use getDashboardStats as a way to validate the session
            // If this fails with 401, we aren't logged in
            await adminApi.getDashboardStats();
            setAdmin({ role: 'admin' }); // Session is valid
        } catch (err) {
            setAdmin(null);
        } finally {
            setLoading(false);
        }
    }, []);

    // --- HELPER: Safely extract error message from FastAPI ---
    const parseError = (err) => {
        if (err.response?.data?.detail) {
            const detail = err.response.data.detail;
            // FastAPI 422 Validation Error returns an Array
            if (Array.isArray(detail)) {
                return detail[0].msg; // Return just the first message string
            }
            // Standard HTTP Error returns a String
            return detail;
        }
        return 'An unexpected error occurred';
    };

    const login = async (email, password) => {
        try {
            setError(null);
            setActionLoading(true);
            const data = await adminApi.login({ email, password });
            setAdmin({ username: data.admin_name, email });
            navigate('/admin/dashboard');
            return { success: true };
        } catch (err) {
            const msg = parseError(err); // <--- Use Helper
            setError(msg);
            return { success: false, error: msg };
        } finally {
            setActionLoading(false);
        }
    };

    const register = async (username, email, password) => {
        try {
            setError(null);
            setActionLoading(true);
            await adminApi.register({ username, email, password });
            // After register, go to login
            navigate('/admin/login');
            return { success: true };
        } catch (err) {
            const msg = parseError(err); // <--- Use Helper
            setError(msg);
            return { success: false, error: msg };
        } finally {
            setActionLoading(false);
        }
    };

    const logout = async () => {
        try {
            await adminApi.logout();
        } finally {
            setAdmin(null);
            navigate('/admin/login');
        }
    };

    const value = {
        admin,
        error,
        loading,        // For Page Loading
        actionLoading,  // For Button Loading
        isAuthenticated: !!admin,
        login,
        register,
        logout
    };

    return (
        <AdminAuthContext.Provider value={value}>
            {children}
        </AdminAuthContext.Provider>
    );
};

// Hook to use Admin Auth
export const useAdminAuth = () => {
    const context = useContext(AdminAuthContext);
    if (!context) {
        throw new Error('useAdminAuth must be used within an AdminAuthProvider');
    }
    return context;
};