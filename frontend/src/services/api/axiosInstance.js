import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const axiosInstance = axios.create({
    baseURL: API_BASE_URL,
    withCredentials: true,
    headers: {
        'Content-Type': 'application/json',
    },
});

axiosInstance.interceptors.response.use(
    (response) => response,
    (error) => {
        const originalRequest = error.config;

        // CRITICAL FIX: Stops the page from reloading if we are already on the login page
        // and get a 401 error (Wrong Password).
        if (
            error.response?.status === 401 && 
            !originalRequest.url.includes('/session/validate') &&
            !originalRequest.url.includes('/login') &&
            window.location.pathname !== '/login' &&
            !window.location.pathname.startsWith('/admin') // <--- ADD THIS LINE
        ) {
            window.location.href = '/login';
        }
        return Promise.reject(error);
    }
);

export default axiosInstance;