import React, { useState, useEffect } from 'react';
import { Password } from 'primereact/password';
import { useNavigate, useSearchParams } from 'react-router'; 
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import RegistrationBackground from '@/assets/registration-background.png'; // Admin BG
import adminApi from '@/services/api/adminApi'; // Use Admin API directly
import usePageTitle from '@/hooks/usePageTitle';

const AdminResetPassword = () => {
    usePageTitle('Admin Reset Password');
    const navigate = useNavigate();
    
    // 1. Get Token from URL
    const [searchParams] = useSearchParams();
    const token = searchParams.get('token');

    const [password, setPassword] = useState('');
    const [confirmPassword, setConfirmPassword] = useState('');
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState({ type: '', content: '' });

    // Redirect if no token is present
    useEffect(() => {
        if (!token) {
            setMsg({ type: 'error', content: 'Invalid or missing reset token.' });
        }
    }, [token]);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setMsg({ type: '', content: '' });

        if (!token) {
            setMsg({ type: 'error', content: 'Invalid or missing reset token.' });
            return;
        }

        if (password !== confirmPassword) {
            setMsg({ type: 'error', content: 'Passwords do not match.' });
            return;
        }

        setLoading(true);

        try {
            // 2. Call Admin API
            await adminApi.resetPassword(token, password);

            setMsg({ type: 'success', content: 'Password reset successful! Redirecting...' });
            
            // 3. Redirect to ADMIN Login
            setTimeout(() => {
                navigate('/admin/login');
            }, 2000);

        } catch (err) {
            const errorText = err.response?.data?.detail || 'Failed to reset password. Token may be expired.';
            setMsg({ type: 'error', content: errorText });
        } finally {
            setLoading(false);
        }
    };

    const isFormValid = password && confirmPassword && password === confirmPassword;

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
                <div className="mb-6 text-center">
                    {/* Admin Badge */}
                    <div className="inline-block px-3 py-1 bg-gray-100 rounded-full text-xs font-bold text-gray-600 mb-4 tracking-wider uppercase">
                        Internal Access Only
                    </div>

                    <Heading level={2} gradient={true} className="text-3xl md:text-3xl mb-2">
                        Set New Password
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600">
                        Enter a new password to secure your admin account.
                    </Text>
                </div>

                {/* Display Messages */}
                {msg.content && (
                    <div className={`mb-4 p-3 rounded-lg text-sm text-center border flex items-center justify-center gap-2 ${
                        msg.type === 'success' 
                        ? 'bg-green-50 text-green-600 border-green-100' 
                        : 'bg-red-50 text-red-600 border-red-100'
                    }`}>
                        <i className={`pi ${msg.type === 'success' ? 'pi-check-circle' : 'pi-exclamation-circle'}`}></i>
                        <span>{msg.content}</span>
                    </div>
                )}

                <form onSubmit={handleSubmit} className="space-y-5">
                    <div className="space-y-2">
                        <label htmlFor="password" className="block text-sm font-medium text-gray-700 ml-1">
                            New Password
                        </label>
                        <Password
                            id="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            placeholder="Enter new password"
                            toggleMask
                            required
                            className="w-full"
                            inputClassName="w-full"
                            disabled={loading || msg.type === 'success'}
                        />
                    </div>

                    <div className="space-y-2">
                        <label htmlFor="confirmPassword" className="block text-sm font-medium text-gray-700 ml-1">
                            Confirm New Password
                        </label>
                        <Password
                            id="confirmPassword"
                            value={confirmPassword}
                            onChange={(e) => setConfirmPassword(e.target.value)}
                            placeholder="Confirm new password"
                            toggleMask
                            feedback={false}
                            required
                            className="w-full"
                            inputClassName="w-full"
                            disabled={loading || msg.type === 'success'}
                        />
                    </div>

                    <div className="pt-2">
                        <PrimaryButton
                            label="Reset Password"
                            type="submit"
                            loading={loading}
                            disabled={loading || !isFormValid || msg.type === 'success'}
                            className="w-full"
                        />
                    </div>
                </form>
            </div>
        </div>
    );
};

export default AdminResetPassword;