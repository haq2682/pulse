import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router'; 
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
            setIsAppLoading(true); 
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
            setIsAppLoading(false); 
        }
    }, []);

    // --- HELPER: Safely extract error message from FastAPI ---
    const parseError = (err, defaultMsg) => {
        if (err.response?.data?.detail) {
            const detail = err.response.data.detail;
            // FastAPI 422 Validation Error returns an Array
            if (Array.isArray(detail)) {
                return detail[0].msg; // Return just the first message string
            }
            // Standard HTTP Error returns a String
            return detail;
        }
        return defaultMsg || 'An unexpected error occurred';
    };

    const login = async (email, password) => {
        try {
            setError(null);
            setIsActionLoading(true); 
            
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
            const message = parseError(err, 'Login failed'); // <--- Use Helper
            setError(message);
            return { success: false, error: message };
        } finally {
            setIsActionLoading(false); 
        }
    };

    const register = async (fullName, email, password) => {
        try {
            setError(null);
            setIsActionLoading(true); 
            
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
            const message = parseError(err, 'Registration failed'); // <--- Use Helper
            setError(message);
            return { success: false, error: message };
        } finally {
            setIsActionLoading(false); 
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
            const message = parseError(err, 'Request failed'); // <--- Use Helper
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
            const message = parseError(err, 'Password reset failed'); // <--- Use Helper
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
        
        // Maps internal loading states to public props
        loading: isActionLoading, 
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