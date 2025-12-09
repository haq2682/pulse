import React from 'react';
import { useNavigate } from 'react-router';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import GradientCircle from '@/components/global/Shapes/GradientCircle';
import HeroBackground from '@/assets/hero-background.png';

const ResetPasswordEmail = () => {
    const navigate = useNavigate();

    const handleTryDifferentEmail = () => {
        navigate('/forgot-password');
    };

    const handleBackToSignIn = () => {
        navigate('/login');
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
                {/* Email Icon */}
                <div className="flex justify-center mb-6">
                    <GradientCircle size={80} className="shadow-lg">
                        <i className="pi pi-envelope text-white text-3xl"></i>
                    </GradientCircle>
                </div>

                {/* Header */}
                <div className="text-center mb-6">
                    <Heading level={2} gradient={true} className="text-2xl md:text-3xl mb-3">
                        Check your Inbox
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600 leading-relaxed">
                        If an account exists for your email, you'll receive an email with instructions to reset your password.
                    </Text>
                </div>

                {/* Help Section */}
                <div className="bg-gray-50 rounded-lg p-4 mb-6">
                    <Heading level={3} gradient={true} className="text-base md:text-lg mb-3 text-center">
                        Didn't get the E-Mail?
                    </Heading>
                    <ul className="space-y-2">
                        <li className="flex items-start">
                            <span className="text-[var(--color-text-primary)] text-sm mr-2">•</span>
                            <Text className="text-sm text-gray-700 m-0">
                                Check spam and promotions folders
                            </Text>
                        </li>
                        <li className="flex items-start">
                            <span className="text-[var(--color-text-primary)] text-sm mr-2">•</span>
                            <Text className="text-sm text-gray-700 m-0">
                                Verify the email address is correct
                            </Text>
                        </li>
                    </ul>
                </div>

                {/* Action Buttons */}
                <div className="flex flex-col sm:flex-row gap-3">
                    <SecondaryButton
                        label="Try different E-Mail"
                        onClick={handleTryDifferentEmail}
                        className="w-full sm:flex-1"
                    />
                    <PrimaryButton
                        label="Back to Sign In"
                        onClick={handleBackToSignIn}
                        className="w-full sm:flex-1"
                    />
                </div>
            </div>
        </div>
    );
};

export default ResetPasswordEmail;