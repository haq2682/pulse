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
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  </PrimeReactProvider>,
)