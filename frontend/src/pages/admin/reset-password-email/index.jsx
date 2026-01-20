import React from 'react';
import { useNavigate } from 'react-router';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import RegistrationBackground from '@/assets/registration-background.png'; // Admin BG

const AdminResetPasswordEmail = () => {
    const navigate = useNavigate();

    const handleTryDifferentEmail = () => {
        navigate('/admin/forgot-password');
    };

    const handleBackToSignIn = () => {
        navigate('/admin/login');
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
            
            {/* Card Container */}
            <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 md:p-10 z-10">
                
                {/* Email Icon */}
                <div className="flex justify-center mb-6">
                    <div className="w-20 h-20 rounded-full bg-blue-50 flex items-center justify-center shadow-sm">
                        <i className="pi pi-envelope text-blue-600 text-3xl"></i>
                    </div>
                </div>

                {/* Header */}
                <div className="text-center mb-6">
                    <div className="inline-block px-3 py-1 bg-gray-100 rounded-full text-xs font-bold text-gray-600 mb-4 tracking-wider uppercase">
                        Internal Access Only
                    </div>
                    
                    <Heading level={2} gradient={true} className="text-2xl md:text-3xl mb-3">
                        Check your Inbox
                    </Heading>
                    <Text className="text-sm md:text-base text-gray-600 leading-relaxed">
                        If an admin account exists for that email, we've sent instructions to reset your password.
                    </Text>
                </div>

                {/* Help Section */}
                <div className="bg-gray-50 rounded-lg p-4 mb-6 border border-gray-100">
                    <Heading level={3} className="text-base font-semibold mb-3 text-center text-gray-800 m-0">
                        Didn't get the E-Mail?
                    </Heading>
                    <ul className="space-y-2 mt-2">
                        <li className="flex items-start justify-center">
                            <span className="text-blue-500 text-sm mr-2">•</span>
                            <Text className="text-sm text-gray-600 m-0">
                                Check spam / junk folder
                            </Text>
                        </li>
                        <li className="flex items-start justify-center">
                            <span className="text-blue-500 text-sm mr-2">•</span>
                            <Text className="text-sm text-gray-600 m-0">
                                Verify the email is correct
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
                        label="Back to Admin Login"
                        onClick={handleBackToSignIn}
                        className="w-full sm:flex-1"
                    />
                </div>
            </div>
        </div>
    );
};

export default AdminResetPasswordEmail;