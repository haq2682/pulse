import React, { useState } from 'react';
import { Password } from 'primereact/password';
import { useNavigate, useSearchParams } from 'react-router'; // NOTE: useSearchParams imports from 'react-router-dom' usually, but 'react-router' in v6
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import HeroBackground from '@/assets/hero-background.png';
// 1. Import Auth Hook
import { useAuth } from '@/context/AuthContext';

const ResetPassword = () => {
    const { resetPassword } = useAuth();
    const navigate = useNavigate();
    
    // 2. Get Token from URL Query Params
    const [searchParams] = useSearchParams();
    const token = searchParams.get('token');

    const [password, setPassword] = useState('');
    const [confirmPassword, setConfirmPassword] = useState('');
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState({ type: '', content: '' });

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

        // 3. Call API
        const result = await resetPassword(token, password);

        setLoading(false);

        if (result.success) {
            setMsg({ type: 'success', content: 'Password reset successful! Redirecting to login...' });
            // Redirect after 2 seconds so user sees the success message
            setTimeout(() => {
                navigate('/login');
            }, 2000);
        } else {
            setMsg({ type: 'error', content: result.error || 'Failed to reset password.' });
        }
    };

    const isFormValid = password && confirmPassword && password === confirmPassword;

    return (
        <div className="min-h-screen flex items-center justify-center bg-gradient-primary p-4">
            <div className="absolute inset-0 z-0">
                <img
                    src={HeroBackground}
                    alt="Background"
                    className="h-full w-full object-cover"
                />
            </div>
            <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 md:p-10 z-1">
                <div className="mb-6">
                    <Heading level={2} gradient={true} className="text-3xl md:text-4xl mb-2">
                        Reset Password
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600">
                        Enter a new password to regain access to your account.
                    </Text>
                </div>

                {/* Display Messages */}
                {msg.content && (
                    <div className={`mb-4 p-3 rounded-lg text-sm text-center border ${
                        msg.type === 'success' 
                        ? 'bg-green-50 text-green-600 border-green-100' 
                        : 'bg-red-50 text-red-600 border-red-100'
                    }`}>
                        {msg.content}
                    </div>
                )}

                <form onSubmit={handleSubmit} className="space-y-5">
                    <div className="space-y-2">
                        <label htmlFor="password" className="block text-sm font-medium text-[var(--color-text-primary)]">
                            Password
                        </label>
                        <Password
                            id="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            placeholder="New Password"
                            toggleMask
                            feedback={false}
                            required
                            className="w-full"
                            inputClassName="w-full"
                            disabled={loading}
                        />
                    </div>

                    <div className="space-y-2">
                        <label htmlFor="confirmPassword" className="block text-sm font-medium text-[var(--color-text-primary)]">
                            Confirm Password
                        </label>
                        <Password
                            id="confirmPassword"
                            value={confirmPassword}
                            onChange={(e) => setConfirmPassword(e.target.value)}
                            placeholder="Confirm New Password"
                            toggleMask
                            feedback={false}
                            required
                            className="w-full"
                            inputClassName="w-full"
                            disabled={loading}
                        />
                    </div>

                    <div className="pt-2">
                        <PrimaryButton
                            label="Confirm Password Change"
                            type="submit"
                            loading={loading}
                            disabled={loading || !isFormValid}
                            className="w-full"
                        />
                    </div>
                </form>
            </div>
        </div>
    );
};

export default ResetPassword;