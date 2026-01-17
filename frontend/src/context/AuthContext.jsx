import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router'; // or 'react-router-dom'
import authApi from '@/services/api/authApi';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(null);
    
    // STATE 1: App Loading (For the full-screen spinner on refresh)
    const [isAppLoading, setIsAppLoading] = useState(true);
    
    // STATE 2: Action Loading (For the button spinner during login)
    const [isActionLoading, setIsActionLoading] = useState(false);
    
    const [error, setError] = useState(null);
    const navigate = useNavigate();

    // Check session on mount
    useEffect(() => {
        checkAuth();
    }, []);

    const checkAuth = useCallback(async () => {
        try {
            setIsAppLoading(true); // Start Full Screen Spinner
            const result = await authApi.validateSession();
            
            if (result.authenticated && result.user) {
                setUser(result.user);
            } else {
                setUser(null);
            }
        } catch (err) {
            console.error('Auth check failed:', err);
            setUser(null);
        } finally {
            setIsAppLoading(false); // Stop Full Screen Spinner
        }
    }, []);

    const login = async (email, password) => {
        try {
            setError(null);
            setIsActionLoading(true); // Start Button Spinner
            
            const data = await authApi.login({ email, password });
            
            const userObj = {
                user_id: data.user_id,
                username: data.username,
                email: data.email
            };
            
            setUser(userObj); 
            navigate('/analytics');
            return { success: true };
        } catch (err) {
            const message = err.response?.data?.detail || 'Login failed';
            setError(message);
            return { success: false, error: message };
        } finally {
            setIsActionLoading(false); // Stop Button Spinner (Even on error)
        }
    };

    const register = async (fullName, email, password) => {
        try {
            setError(null);
            setIsActionLoading(true); // Start Button Spinner
            
            const data = await authApi.register({ 
                username: fullName, 
                email, 
                password 
            });
            
            const userObj = {
                user_id: data.user_id,
                username: data.username,
                email: data.email
            };

            setUser(userObj);
            navigate('/analytics');
            return { success: true };
        } catch (err) {
            const message = err.response?.data?.detail || 'Registration failed';
            setError(message);
            return { success: false, error: message };
        } finally {
            setIsActionLoading(false); // Stop Button Spinner
        }
    };

    const logout = async () => {
        try {
            await authApi.logout();
        } catch (err) {
            console.error('Logout error:', err);
        } finally {
            setUser(null);
            navigate('/login');
        }
    };

    const forgotPassword = async (email) => {
        try {
            setError(null);
            const result = await authApi.forgotPassword(email);
            return { success: true, message: result.message };
        } catch (err) {
            const message = err.response?.data?.detail || 'Request failed';
            setError(message);
            return { success: false, error: message };
        }
    };

    const resetPassword = async (token, newPassword) => {
        try {
            setError(null);
            const result = await authApi.resetPassword(token, newPassword);
            return { success: true, message: result.message };
        } catch (err) {
            const message = err.response?.data?.detail || 'Password reset failed';
            setError(message);
            return { success: false, error: message };
        }
    };

    const loginWithGoogle = () => {
        window.location.href = authApi.getGoogleAuthUrl();
    };

    const value = {
        user,
        error,
        isAuthenticated: !!user,
        
        // This maps the INTERNAL 'action' loading to the PUBLIC 'loading' prop
        // So your Login Page sees 'loading' as true when clicking buttons.
        loading: isActionLoading, 
        
        // This is a NEW prop specifically for the Route Guards
        appLoading: isAppLoading,

        login,
        register,
        logout,
        forgotPassword,
        resetPassword,
        loginWithGoogle,
        checkAuth,
        clearError: () => setError(null)
    };

    return (
        <AuthContext.Provider value={value}>
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};

export default AuthContext;