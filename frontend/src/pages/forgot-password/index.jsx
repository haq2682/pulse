import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';
import RegistrationBackground from '@/assets/registration-background.png';

const ForgotPassword = () => {
    const [email, setEmail] = useState('');

    const handleSubmit = (e) => {
        e.preventDefault();
        console.log('Reset email sent to:', email);
        // Add your forgot password logic here
    };

    return (
        <div className="min-h-screen bg-gray-50 flex">
            <div className="absolute inset-0 z-0">
                <img
                    src={RegistrationBackground}
                    alt="Background"
                    className="h-full w-full object-cover"
                />
            </div>
            {/* Left Section - Hidden on mobile/tablet */}
            <div className="hidden lg:flex lg:w-1/2 p-12 flex-col justify-between z-1">
                <div>
                    <Heading level={2} white={true} className="text-4xl mb-4">
                        Pulse Analytics
                    </Heading>
                    <Text className="text-white text-lg opacity-90 mb-8">
                        Transform your e-commerce data into actionable insights
                    </Text>
                </div>

                <div className="mx-15">
                    <div className="flex items-center">
                        <div>
                            <Heading level={3} white={true} className="text-5xl">
                                Reset your password
                            </Heading>
                            <Text className="text-white text-xl mt-5">
                                Enter your email address and we'll send you a link to reset your password.
                            </Text>
                        </div>
                    </div>
                </div>

                <div className="text-white/70 text-sm">
                    © 2025 Pulse Analytics. All rights reserved.
                </div>
            </div>

            {/* Right Section - Form */}
            <div className="w-full lg:w-1/2 flex items-center justify-center p-6 sm:p-8 lg:p-12 z-20 bg-neutral-50 lg:rounded-l-4xl">
                <div className="w-full max-w-md">
                    {/* Mobile/Tablet Header */}
                    <div className="lg:hidden mb-8">
                        <Heading level={2} gradient={true} className="text-3xl mb-2">
                            Pulse Analytics
                        </Heading>
                    </div>

                    {/* Form Container */}
                    <div className="bg-white rounded-2xl shadow-lg p-8 sm:p-10">
                        <div className="mb-8">
                            <Heading level={1} gradient={true} className="text-3xl mb-2">
                                Forgot Password?
                            </Heading>
                            <Text className="text-base">
                                Don't worry! Enter your email and we'll send you a reset link.
                            </Text>
                        </div>

                        <form onSubmit={handleSubmit} className="space-y-5">
                            {/* Email */}
                            <div>
                                <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-2">
                                    Email Address
                                </label>
                                <InputText
                                    id="email"
                                    name="email"
                                    type="email"
                                    value={email}
                                    onChange={(e) => setEmail(e.target.value)}
                                    placeholder="Enter your email"
                                    className="w-full"
                                    required
                                />
                            </div>

                            {/* Submit Button */}
                            <PrimaryButton
                                label="Send Reset Link"
                                type="submit"
                                className="w-full text-base font-semibold"
                            />

                            {/* Back to Login Link */}
                            <div className="text-center">
                                <Text className="text-sm inline">
                                    Remember your password?{' '}
                                </Text>
                                <CustomLink href="/login" className="text-sm font-semibold cursor-pointer">
                                    Back to Login
                                </CustomLink>
                            </div>
                        </form>
                    </div>

                    {/* Mobile/Tablet Footer */}
                    <div className="lg:hidden mt-8 text-center">
                        <Text className="text-sm text-gray-500">
                            © 2025 Pulse Analytics. All rights reserved.
                        </Text>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ForgotPassword;