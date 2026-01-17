import React from 'react';
import { Button } from 'primereact/button';
import { useAuth } from '@/context/AuthContext';

const GoogleLoginButton = ({ label = "Continue with Google", className = "" }) => {
    const { loginWithGoogle } = useAuth();

    return (
        <Button
            label={label}
            icon="pi pi-google"
            iconPos="left"
            onClick={loginWithGoogle}
            className={`w-full bg-white text-gray-700 border border-gray-300 
                       hover:bg-gray-50 ${className}`}
            style={{
                background: 'white',
                color: '#374151',
                border: '1px solid #D1D5DB'
            }}
        />
    );
};

export default GoogleLoginButton;