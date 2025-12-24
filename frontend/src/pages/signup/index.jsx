import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Password } from 'primereact/password';
import { Checkbox } from 'primereact/checkbox';
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';
import RegistrationBackground from '@/assets/registration-background.png';

const Signup = () => {
    const [formData, setFormData] = useState({
        fullName: '',
        email: '',
        password: '',
        confirmPassword: '',
        agreeToTerms: false
    });

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({ ...prev, [name]: value }));
    };

    const handleCheckboxChange = (e) => {
        setFormData(prev => ({ ...prev, agreeToTerms: e.checked }));
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
                                Create your account and turn data into decisions
                            </Heading>
                            <Text className="text-white text-xl mt-5">
                                Set up Pulse to track revenue, cohorts, inventory health,
                                and AI forecasts across your e‑commerce business.
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
                                Create Account
                            </Heading>
                            <Text className="text-base">Join us and start analyzing your data</Text>
                        </div>

                        <form onSubmit={handleSubmit} className="space-y-5">
                            {/* Full Name */}
                            <div>
                                <label htmlFor="fullName" className="block text-sm font-medium text-gray-700 mb-2">
                                    Full Name
                                </label>
                                <InputText
                                    id="fullName"
                                    name="fullName"
                                    value={formData.fullName}
                                    onChange={handleInputChange}
                                    placeholder="Enter your full name"
                                    className="w-full"
                                    required
                                />
                            </div>

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

                            {/* Confirm Password */}
                            <div>
                                <label htmlFor="confirmPassword" className="block text-sm font-medium text-gray-700 mb-2">
                                    Confirm Password
                                </label>
                                <Password
                                    id="confirmPassword"
                                    name="confirmPassword"
                                    value={formData.confirmPassword}
                                    onChange={handleInputChange}
                                    placeholder="Confirm your password"
                                    className="w-full"
                                    inputClassName="w-full"
                                    toggleMask
                                    required
                                    feedback={false}
                                />
                            </div>

                            {/* Terms and Conditions */}
                            <div className="flex items-center gap-3">
                                <Checkbox
                                    inputId="agreeToTerms"
                                    name="agreeToTerms"
                                    checked={formData.agreeToTerms}
                                    onChange={handleCheckboxChange}
                                    required
                                />
                                <label htmlFor="agreeToTerms" className="text-sm text-gray-600">
                                    I agree to the{' '}
                                    <CustomLink className="text-sm font-medium cursor-pointer">
                                        Terms of Service
                                    </CustomLink>
                                    {' '}and{' '}
                                    <CustomLink className="text-sm font-medium cursor-pointer">
                                        Privacy Policy
                                    </CustomLink>
                                </label>
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
                                    Already have an account?{' '}
                                </Text>
                                <CustomLink className="text-sm font-semibold cursor-pointer">
                                    Sign In
                                </CustomLink>
                            </div>

                            <div className="flex items-center my-0">
                                <hr className="w-1/2 text-neutral-300 mb-3 mx-4" />
                                <Text className="text-center text-sm text-gray-500 mb-4">or</Text>
                                <hr className="w-1/2 text-neutral-300 mb-3 mx-4" />
                            </div>

                            <PrimaryButton
                                label="Sign Up with Google"
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

export default Signup;