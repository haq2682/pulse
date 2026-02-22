import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Password } from 'primereact/password';
import { useNavigate } from 'react-router'; // <--- 1. Import useNavigate
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';
import RegistrationBackground from '@/assets/registration-background.png';
// Import Auth Hook
import { useAuth } from '@/context/AuthContext';
import usePageTitle from '@/hooks/usePageTitle';

const Login = () => {
    // Get login functions
    usePageTitle('Log In'); // <--- Set Page Title
    const { login, loginWithGoogle, loading, error } = useAuth();
    const navigate = useNavigate(); // <--- 2. Initialize Hook
    
    const [formData, setFormData] = useState({
        email: '',
        password: '',
    });

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({ ...prev, [name]: value }));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        // Call Login API
        await login(formData.email, formData.password);
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

                        {/* Error Message Display */}
                        {error && (
                            <div className="mb-4 p-3 bg-red-50 text-red-600 rounded-lg text-sm text-center border border-red-100">
                                {error}
                            </div>
                        )}

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
                                {/* <--- 3. ADDED: Flex container for Label + Forgot Password Link ---> */}
                                <div className="flex items-center justify-between mb-2">
                                    <label htmlFor="password" className="block text-sm font-medium text-gray-700">
                                        Password
                                    </label>
                                    <CustomLink 
                                        href="/forgot-password"
                                        onClick={(e) => {
                                            e.preventDefault();
                                            navigate('/forgot-password');
                                        }}
                                        className="text-sm font-semibold text-[var(--color-primary)] hover:opacity-80 cursor-pointer"
                                    >
                                        Forgot Password?
                                    </CustomLink>
                                </div>

                                <Password
                                    id="password"
                                    name="password"
                                    value={formData.password}
                                    onChange={handleInputChange}
                                    placeholder="Enter your password"
                                    className="w-full"
                                    inputClassName="w-full"
                                    toggleMask
                                    required
                                    feedback={false}
                                />
                            </div>

                            {/* Submit Button */}
                            <PrimaryButton
                                label="Log In" 
                                type="submit"
                                className="w-full text-base font-semibold"
                                loading={loading} 
                            />

                            {/* Sign Up Link */}
                            <div className="text-center">
                                <Text className="text-sm inline">
                                    New to Pulse Analytics?{' '}
                                </Text>
                                <CustomLink href="/signup" className="text-sm font-semibold cursor-pointer">
                                    Sign Up
                                </CustomLink>
                            </div>

                            <div className="flex items-center my-0">
                                <hr className="w-1/2 text-neutral-300 mb-3 mx-4" />
                                <Text className="text-center text-sm text-gray-500 mb-4">or</Text>
                                <hr className="w-1/2 text-neutral-300 mb-3 mx-4" />
                            </div>

                            <PrimaryButton
                                label="Log In with Google"
                                type="button" 
                                onClick={loginWithGoogle}
                                iconPos="right"
                                icon="pi pi-google"
                                className="w-full text-base font-semibold"
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