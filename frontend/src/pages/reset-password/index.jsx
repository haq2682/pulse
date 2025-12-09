import React, { useState } from 'react';
import { Password } from 'primereact/password';
import { useNavigate } from 'react-router';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import HeroBackground from '@/assets/hero-background.png';

const ResetPassword = () => {
    const [password, setPassword] = useState('');
    const [confirmPassword, setConfirmPassword] = useState('');
    const [loading, setLoading] = useState(false);
    const navigate = useNavigate();

    const handleSubmit = async (e) => {
        e.preventDefault();

        // Validation
        if (password !== confirmPassword) {
            // TODO: Show error message - passwords don't match
            return;
        }

        setLoading(true);

        // TODO: Add your reset password API call here
        setTimeout(() => {
            setLoading(false);
            // Navigate to login page on success
            navigate('/login');
        }, 1500);
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
            {/* Card Container */}
            <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 md:p-10 z-1">
                {/* Header */}
                <div className="mb-6">
                    <Heading level={2} gradient={true} className="text-3xl md:text-4xl mb-2">
                        Reset Password
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600">
                        Enter a new password to regain access to your account.
                    </Text>
                </div>

                {/* Form */}
                <form onSubmit={handleSubmit} className="space-y-5">
                    {/* Password Input */}
                    <div className="space-y-2">
                        <label
                            htmlFor="password"
                            className="block text-sm font-medium text-[var(--color-text-primary)]"
                        >
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

                    {/* Confirm Password Input */}
                    <div className="space-y-2">
                        <label
                            htmlFor="confirmPassword"
                            className="block text-sm font-medium text-[var(--color-text-primary)]"
                        >
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

                    {/* Submit Button */}
                    <div className="pt-2">
                        <PrimaryButton
                            label="Confirm Password Change"
                            onClick={handleSubmit}
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