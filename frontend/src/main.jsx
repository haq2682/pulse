import { createRoot } from 'react-dom/client'
import { PrimeReactProvider } from 'primereact/api';
import 'primereact/resources/themes/lara-light-green/theme.css';
import 'primeicons/primeicons.css';
import './styles/theme.css';
import './index.css'
import { BrowserRouter, Routes, Route } from "react-router";
import { ThemeProvider } from "@/context/ThemeContext";
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

// PrimeReact configuration
const primeReactConfig = {
  ripple: true, // Enable ripple effect globally
  inputStyle: 'outlined', // or 'filled'
  locale: 'en', // Default locale
};

createRoot(document.getElementById('root')).render(
  <PrimeReactProvider value={primeReactConfig}>
    <ThemeProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Landing />} />
          <Route path="/theme" element={<ThemeReference />} />
          <Route path="/theme2" element={<ThemeReferenceV2 />} />
          <Route path="/signup" element={<Signup />} />
          <Route path="/login" element={<Login />} />
          <Route path="/forgot-password" element={<ForgotPassword />} />
          <Route path="/reset-password" element={<ResetPassword />} />
          <Route path="/reset-password-email" element={<ResetPasswordEmail />} />
          <Route path="/analytics" element={<Dashboard />} />
          <Route path="/onboarding/business" element={<AddBusiness />} />
          <Route path="/onboarding/data-type" element={<DataType />} />
          <Route path="/onboarding/connect" element={<Connect />} />
          <Route path="/onboarding/mapping" element={<Mapping />} />
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  </PrimeReactProvider>,
)