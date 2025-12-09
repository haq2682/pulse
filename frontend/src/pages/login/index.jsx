import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Password } from 'primereact/password';
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';
import RegistrationBackground from '@/assets/registration-background.png';

const Login = () => {
    const [formData, setFormData] = useState({
        email: '',
        password: '',
    });

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({ ...prev, [name]: value }));
    };

    const handleSubmit = (e) => {
        e.preventDefault();
        console.log('Form submitted:', formData);
        // Add your signup logic here
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
                                Sign in to your workspace
                            </Heading>
                            <Text className="text-white text-xl mt-5">
                                Access revenue trends, RFM segments, cohorts,
                                forecasts, and inventory alerts in one place.
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
                                Welcome Back
                            </Heading>
                            <Text className="text-base">Enter your credentials to continue</Text>
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
                                    value={formData.email}
                                    onChange={handleInputChange}
                                    placeholder="Enter your email"
                                    className="w-full"
                                    required
                                />
                            </div>

                            {/* Password */}
                            <div className="w-full">
                                <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-2">
                                    Password
                                </label>
                                <Password
                                    id="password"
                                    name="password"
                                    value={formData.password}
                                    onChange={handleInputChange}
                                    placeholder="Create a password"
                                    className="w-full"
                                    inputClassName="w-full"
                                    toggleMask
                                    required
                                    feedback={false}
                                />
                            </div>

                            {/* Submit Button */}
                            <PrimaryButton
                                label="Create Account"
                                type="submit"
                                className="w-full text-base font-semibold"
                                disabled={!formData.agreeToTerms}
                            />

                            {/* Sign In Link */}
                            <div className="text-center">
                                <Text className="text-sm inline">
                                    New to Pulse Analytics?{' '}
                                </Text>
                                <CustomLink className="text-sm font-semibold cursor-pointer">
                                    Log In
                                </CustomLink>
                            </div>

                            <div className="flex items-center my-0">
                                <hr className="w-1/2 text-neutral-300 mb-3 mx-4" />
                                <Text className="text-center text-sm text-gray-500 mb-4">or</Text>
                                <hr className="w-1/2 text-neutral-300 mb-3 mx-4" />
                            </div>

                            <PrimaryButton
                                label="Log In with Google"
                                type="submit"
                                iconPos="right"
                                icon="pi pi-google"
                                className="w-full text-base font-semibold"
                                disabled={!formData.agreeToTerms}
                            />
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

export default Login;