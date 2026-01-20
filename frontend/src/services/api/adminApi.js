import axiosInstance from './axiosInstance';

export const adminApi = {
    // Admin Login
    login: async (credentials) => {
        const response = await axiosInstance.post('/admin/login', credentials);
        return response.data;
    },

    // Admin Register
    register: async (adminData) => {
        const response = await axiosInstance.post('/admin/register', adminData);
        return response.data;
    },

    // Admin Logout
    logout: async () => {
        const response = await axiosInstance.post('/admin/logout');
        return response.data;
    },

    // Forgot Password
    forgotPassword: async (email) => {
        const response = await axiosInstance.post('/admin/forgot-password', { email });
        return response.data;
    },

    // Reset Password
    resetPassword: async (token, newPassword) => {
        const response = await axiosInstance.post('/admin/reset-password', {
            token,
            new_password: newPassword
        });
        return response.data;
    },


    // Get Dashboard Stats (Used for Data & Session Validation)
    getDashboardStats: async () => {
        const response = await axiosInstance.get('/admin/dashboard-stats');
        return response.data;
    }
};

export default adminApi;