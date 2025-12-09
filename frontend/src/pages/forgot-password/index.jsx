import React, { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { useNavigate } from 'react-router';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import HeroBackground from '@/assets/hero-background.png';

const ForgotPassword = () => {
    const [email, setEmail] = useState('');
    const [loading, setLoading] = useState(false);
    const navigate = useNavigate();

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);

        // TODO: Add your forgot password API call here
        setTimeout(() => {
            setLoading(false);
            // navigate to success page or show success message
        }, 1500);
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
            {/* Card Container */}
            <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 md:p-10 z-1">
                {/* Header */}
                <div className="text-center mb-6">
                    <Heading level={2} gradient={true} className="text-3xl md:text-4xl mb-2">
                        Forgot Password?
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600">
                        We'll email you a secure link to reset your password
                    </Text>
                </div>

                {/* Form */}
                <form onSubmit={handleSubmit} className="space-y-6">
                    {/* Email Input */}
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

                    {/* Submit Button */}
                    <PrimaryButton
                        label="Send Link"
                        onClick={handleSubmit}
                        loading={loading}
                        disabled={loading || !email}
                        className="w-full"
                    />

                    {/* Return to Login Link */}
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