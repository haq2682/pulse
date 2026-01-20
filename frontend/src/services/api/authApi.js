import axiosInstance from './axiosInstance';

export const authApi = {
    // Register new user
    register: async (userData) => {
        const response = await axiosInstance.post('/auth/register', userData);
        return response.data;
    },

    // Login with email and password
    login: async (credentials) => {
        const response = await axiosInstance.post('/auth/login', credentials);
        return response.data;
    },

    // Logout
    logout: async () => {
        const response = await axiosInstance.post('/auth/logout');
        return response.data;
    },

    // Get current user
    getCurrentUser: async () => {
        const response = await axiosInstance.get('/auth/me');
        return response.data;
    },

    // Validate session
    validateSession: async () => {
        const response = await axiosInstance.get('/auth/session/validate');
        return response.data;
    },

    // Request password reset
    forgotPassword: async (email) => {
        const response = await axiosInstance.post('/auth/forgot-password', { email });
        return response.data;
    },

    // Reset password with token
    resetPassword: async (token, newPassword) => {
        const response = await axiosInstance.post('/auth/reset-password', {
            token,
            new_password: newPassword
        });
        return response.data;
    },

    // Get Google OAuth URL
    getGoogleAuthUrl: () => {
        return `${import.meta.env.VITE_API_URL || 'http://localhost:8000'}/auth/google`;
    }
};

export default authApi;