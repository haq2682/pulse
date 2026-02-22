import { createRoot } from 'react-dom/client'
import { PrimeReactProvider } from 'primereact/api';
import 'primereact/resources/themes/lara-light-green/theme.css';
import 'primeicons/primeicons.css';
import './styles/theme.css';
import './index.css'
import { BrowserRouter, Routes, Route } from "react-router";
import { ThemeProvider } from "@/context/ThemeContext";
import { AuthProvider } from '@/context/AuthContext'; 
import { AdminAuthProvider } from '@/context/AdminAuthContext';
import { PipelineProgressProvider } from '@/context/PipelineProgressContext';

// Guards
import ProtectedAdminRoute from '@/components/auth/ProtectedAdminRoute';
import GuestAdminRoute from '@/components/auth/GuestAdminRoute';
import ProtectedRoute from '@/components/auth/ProtectedRoute';
import GuestRoute from '@/components/auth/GuestRoute';

// User Pages
import Landing from "@/pages/landing/index.jsx";
import Signup from "@/pages/signup/index.jsx";
import Login from "@/pages/login/index.jsx";
import ForgotPassword from '@/pages/forgot-password/index.jsx';
import ResetPassword from '@/pages/reset-password/index.jsx';
import ResetPasswordEmail from '@/pages/reset-password-email/index.jsx';
import Dashboard from '@/pages/dashboard/index.jsx';
import AddBusiness from '@/pages/onboarding/business/index.jsx';
import DataType from '@/pages/onboarding/data-type/index.jsx';
import Connect from '@/pages/onboarding/connect/index.jsx';
import Mapping from '@/pages/onboarding/mapping/index.jsx';

// Admin Pages
import AdminLogin from "@/pages/admin/login/index.jsx";
import AdminSignup from "@/pages/admin/signup/index.jsx";
import AdminDashboard from "@/pages/admin/dashboard/index.jsx";
// --- NEW ADMIN PAGES ---
import AdminForgotPassword from "@/pages/admin/forgot-password/index.jsx";
import AdminResetPassword from "@/pages/admin/reset-password/index.jsx";
import AdminResetPasswordEmail from "@/pages/admin/reset-password-email/index.jsx";

const primeReactConfig = {
  ripple: true,
  inputStyle: 'outlined',
  locale: 'en',
};

createRoot(document.getElementById('root')).render(
  <PrimeReactProvider value={primeReactConfig}>
    <ThemeProvider>
      <BrowserRouter>
        {/* AuthProvider must be INSIDE BrowserRouter */}
        <AuthProvider>
          {/* PipelineProgressProvider for pipeline tracking */}
          <PipelineProgressProvider>
            {/* Nest AdminAuthProvider here so it can use Router and Auth hooks if needed */}
            <AdminAuthProvider>
                <Routes>
                    {/* PUBLIC ROUTES */}
                    <Route path="/" element={<GuestRoute><Landing /></GuestRoute>} />

                    {/* USER GUEST ROUTES */}
                    <Route path="/signup" element={<GuestRoute><Signup /></GuestRoute>} />
                    <Route path="/login" element={<GuestRoute><Login /></GuestRoute>} />
                    <Route path="/forgot-password" element={<GuestRoute><ForgotPassword /></GuestRoute>} />
                    <Route path="/reset-password" element={<GuestRoute><ResetPassword /></GuestRoute>} />
                    <Route path="/reset-password-email" element={<GuestRoute><ResetPasswordEmail /></GuestRoute>} />

                    {/* USER PROTECTED ROUTES */}
                    <Route path="/analytics/:businessId?/:subpage1?/:subpage2?" element={<ProtectedRoute><Dashboard /></ProtectedRoute>} />
                    <Route path="/onboarding/business/:onboardingId" element={<ProtectedRoute><AddBusiness /></ProtectedRoute>} />
                    <Route path="/onboarding/data-type/:onboardingId" element={<ProtectedRoute><DataType /></ProtectedRoute>} />
                    <Route path="/onboarding/connect/:onboardingId" element={<ProtectedRoute><Connect /></ProtectedRoute>} />
                    <Route path="/onboarding/mapping/:onboardingId" element={<ProtectedRoute><Mapping /></ProtectedRoute>} />

                    {/* --- ADMIN ROUTES --- */}
                    
                    {/* Admin Guest Routes (Login/Signup/Recovery) */}
                    <Route 
                        path="/admin/login" 
                        element={
                            <GuestAdminRoute>
                                <AdminLogin />
                            </GuestAdminRoute>
                        } 
                    />
                    <Route 
                        path="/admin/signup" 
                        element={
                            <GuestAdminRoute>
                                <AdminSignup />
                            </GuestAdminRoute>
                        } 
                    />
                    <Route 
                        path="/admin/forgot-password" 
                        element={
                            <GuestAdminRoute>
                                <AdminForgotPassword />
                            </GuestAdminRoute>
                        } 
                    />
                    <Route 
                        path="/admin/reset-password" 
                        element={
                            <GuestAdminRoute>
                                <AdminResetPassword />
                            </GuestAdminRoute>
                        } 
                    />
                    <Route 
                        path="/admin/reset-password-email" 
                        element={
                            <GuestAdminRoute>
                                <AdminResetPasswordEmail />
                            </GuestAdminRoute>
                        } 
                    />

                    {/* Admin Protected Routes (Dashboard) */}
                    <Route 
                        path="/admin/dashboard" 
                        element={
                            <ProtectedAdminRoute>
                                <AdminDashboard />
                            </ProtectedAdminRoute>
                        } 
                    />

                </Routes>
           </AdminAuthProvider>
          </PipelineProgressProvider>
        </AuthProvider>
      </BrowserRouter>
    </ThemeProvider>
  </PrimeReactProvider>
);