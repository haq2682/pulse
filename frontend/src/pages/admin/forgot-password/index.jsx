import React, { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { useNavigate } from 'react-router';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import RegistrationBackground from '@/assets/registration-background.png'; // Using Admin BG
import adminApi from '@/services/api/adminApi'; // Direct API call

const AdminForgotPassword = () => {
    const [email, setEmail] = useState('');
    const [loading, setLoading] = useState(false);
    const [status, setStatus] = useState({ error: '', success: '' });
    
    const navigate = useNavigate();

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setStatus({ error: '', success: '' });

        try {
            // 1. Call Admin API directly
            await adminApi.forgotPassword(email);
            // 2. SUCCESS: Show message
            setStatus({ success: 'Reset link sent! Check your inbox.', error: '' });

            navigate('/admin/reset-password-email');
            
            // 3. Redirect to ADMIN Login after short delay
            
        } catch (err) {
            // 3. ERROR: Show error
            const msg = err.response?.data?.detail || 'Failed to send reset link.';
            setStatus({ error: msg, success: '' });
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-gray-900 p-4 relative overflow-hidden">
            {/* Background Overlay */}
            <div className="absolute inset-0 z-0 opacity-20">
                <img
                    src={RegistrationBackground}
                    alt="Background"
                    className="h-full w-full object-cover"
                />
            </div>
            
            <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 md:p-10 z-10">
                <div className="text-center mb-6">
                    {/* Admin Badge */}
                    <div className="inline-block px-3 py-1 bg-gray-100 rounded-full text-xs font-bold text-gray-600 mb-4 tracking-wider uppercase">
                        Internal Access Only
                    </div>
                    
                    <Heading level={2} gradient={true} className="text-3xl md:text-3xl mb-2">
                        Admin Recovery
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600">
                        Enter your admin email to reset password
                    </Text>
                </div>

                {/* Display Messages */}
                {status.error && (
                    <div className="mb-4 p-3 bg-red-50 text-red-600 rounded-lg text-sm text-center border border-red-100 flex items-center justify-center gap-2">
                        <i className="pi pi-exclamation-circle"></i>
                        <span>{status.error}</span>
                    </div>
                )}
                {status.success && (
                    <div className="mb-4 p-3 bg-green-50 text-green-600 rounded-lg text-sm text-center border border-green-100 flex items-center justify-center gap-2">
                        <i className="pi pi-check-circle"></i>
                        <span>{status.success}</span>
                    </div>
                )}

                <form onSubmit={handleSubmit} className="space-y-6">
                    <div className="space-y-2">
                        <label
                            htmlFor="email"
                            className="block text-sm font-medium text-gray-700 ml-1"
                        >
                            Admin Email Address
                        </label>
                        <InputText
                            id="email"
                            type="email"
                            placeholder="admin@pulse.com"
                            value={email}
                            onChange={(e) => setEmail(e.target.value)}
                            required
                            className="w-full"
                            disabled={loading || status.success}
                        />
                    </div>

                    <PrimaryButton
                        label="Send Reset Link"
                        type="submit"
                        loading={loading}
                        disabled={loading || !email || status.success}
                        className="w-full"
                    />

                    <div className="text-center pt-2">
                        <CustomLink
                            href="/admin/login"
                            onClick={(e) => {
                                e.preventDefault();
                                navigate('/admin/login');
                            }}
                            className="text-sm font-semibold text-gray-600 hover:text-gray-900"
                        >
                            Return to Admin Login
                        </CustomLink>
                    </div>
                </form>
            </div>
        </div>
    );
};

export default AdminForgotPassword;