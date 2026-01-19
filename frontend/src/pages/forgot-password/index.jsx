import React, { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { useNavigate } from 'react-router';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import HeroBackground from '@/assets/hero-background.png';
import { useAuth } from '@/context/AuthContext';

const ForgotPassword = () => {
    const { forgotPassword } = useAuth();
    const [email, setEmail] = useState('');
    const [loading, setLoading] = useState(false);
    const [errorMsg, setErrorMsg] = useState(''); // Only need error state now
    
    const navigate = useNavigate();

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setErrorMsg('');

        // 1. Call API
        const result = await forgotPassword(email);

        setLoading(false);

        if (result.success) {
            // 2. SUCCESS: Redirect to the confirmation page immediately
            navigate('/reset-password-email');
        } else {
            // 3. ERROR: Stay on this page and show error
            setErrorMsg(result.error || 'Failed to send reset link.');
        }
    };

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
                <div className="text-center mb-6">
                    <Heading level={2} gradient={true} className="text-3xl md:text-4xl mb-2">
                        Forgot Password?
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600">
                        We'll email you a secure link to reset your password
                    </Text>
                </div>

                {/* Display Error Message Only */}
                {errorMsg && (
                    <div className="mb-4 p-3 bg-red-50 text-red-600 rounded-lg text-sm text-center border border-red-100">
                        {errorMsg}
                    </div>
                )}

                <form onSubmit={handleSubmit} className="space-y-6">
                    <div className="space-y-2">
                        <label
                            htmlFor="email"
                            className="block text-sm font-medium text-[var(--color-text-primary)]"
                        >
                            E-Mail Address
                        </label>
                        <InputText
                            id="email"
                            type="email"
                            placeholder="E-Mail"
                            value={email}
                            onChange={(e) => setEmail(e.target.value)}
                            required
                            className="w-full"
                            disabled={loading}
                        />
                    </div>

                    <PrimaryButton
                        label="Send Link"
                        type="submit"
                        loading={loading}
                        disabled={loading || !email}
                        className="w-full"
                    />

                    <div className="text-center pt-2">
                        <CustomLink
                            href="/login"
                            onClick={(e) => {
                                e.preventDefault();
                                navigate('/login');
                            }}
                            gradient={true}
                            className="text-sm md:text-base"
                        >
                            Return to Log In
                        </CustomLink>
                    </div>
                </form>
            </div>
        </div>
    );
};

export default ForgotPassword;