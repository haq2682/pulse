import { createRoot } from 'react-dom/client'
import { PrimeReactProvider } from 'primereact/api';
import 'primereact/resources/themes/lara-light-green/theme.css';
import 'primeicons/primeicons.css';
import './styles/theme.css';
import './index.css'
import { BrowserRouter, Routes, Route } from "react-router";
import { ThemeProvider } from "@/context/ThemeContext";
import { AuthProvider } from '@/context/AuthContext'; // IMPORT THIS

// Import Guards
import ProtectedRoute from '@/components/auth/ProtectedRoute';
import GuestRoute from '@/components/auth/GuestRoute';

import Landing from "@/pages/landing/index.jsx";
import ThemeReference from "@/pages/ThemeReference/index.jsx";
import ThemeReferenceV2 from "@/pages/ThemeReferenceV2/index.jsx";
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
            <Routes>
            {/* PUBLIC ROUTES */}
            <Route path="/" element={<Landing />} />
            <Route path="/theme" element={<ThemeReference />} />
            <Route path="/theme2" element={<ThemeReferenceV2 />} />

            {/* GUEST ROUTES (Redirect to Dashboard if logged in) */}
            <Route path="/signup" element={<GuestRoute><Signup /></GuestRoute>} />
            <Route path="/login" element={<GuestRoute><Login /></GuestRoute>} />
            <Route path="/forgot-password" element={<GuestRoute><ForgotPassword /></GuestRoute>} />
            <Route path="/reset-password" element={<GuestRoute><ResetPassword /></GuestRoute>} />
            <Route path="/reset-password-email" element={<GuestRoute><ResetPasswordEmail /></GuestRoute>} />

            {/* PROTECTED ROUTES (Redirect to Login if NOT logged in) */}
            <Route path="/analytics" element={<ProtectedRoute><Dashboard /></ProtectedRoute>} />
            <Route path="/onboarding/business" element={<ProtectedRoute><AddBusiness /></ProtectedRoute>} />
            <Route path="/onboarding/data-type" element={<ProtectedRoute><DataType /></ProtectedRoute>} />
            <Route path="/onboarding/connect" element={<ProtectedRoute><Connect /></ProtectedRoute>} />
            <Route path="/onboarding/mapping" element={<ProtectedRoute><Mapping /></ProtectedRoute>} />
            </Routes>
        </AuthProvider>
      </BrowserRouter>
    </ThemeProvider>
  </PrimeReactProvider>
);