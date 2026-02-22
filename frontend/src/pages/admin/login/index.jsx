import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Password } from 'primereact/password';
import { useNavigate } from 'react-router'; // <--- 1. Import Hook
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';
import RegistrationBackground from '@/assets/registration-background.png';
import { useAdminAuth } from '@/context/AdminAuthContext';
import usePageTitle from '@/hooks/usePageTitle';

const AdminLogin = () => {
    usePageTitle('Admin Log In'); // <--- Set Page Title
    const { login, actionLoading, error } = useAdminAuth();
    const navigate = useNavigate(); // <--- 2. Initialize Hook
    
    const [formData, setFormData] = useState({ email: '', password: '' });

    const handleSubmit = async (e) => {
        e.preventDefault();
        await login(formData.email, formData.password);
    };

    return (
        <div className="min-h-screen bg-gray-900 flex">
            <div className="absolute inset-0 z-0 opacity-20">
                <img src={RegistrationBackground} alt="Background" className="h-full w-full object-cover" />
            </div>

            <div className="w-full flex items-center justify-center p-4 z-10">
                <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 sm:p-10">
                    <div className="mb-8 text-center">
                        <div className="inline-block px-3 py-1 bg-gray-100 rounded-full text-xs font-bold text-gray-600 mb-4 tracking-wider">
                            INTERNAL ACCESS ONLY
                        </div>
                        <Heading level={1} gradient={true} className="text-3xl mb-2">
                            Admin Portal
                        </Heading>
                        <Text className="text-base">Secure login for Pulse Administrators</Text>
                    </div>

                    {error && (
                        <div className="mb-4 p-3 bg-red-50 text-red-600 rounded-lg text-sm text-center border border-red-100">
                            {error}
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="space-y-5">
                        <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">Email Address</label>
                            <InputText 
                                value={formData.email}
                                onChange={(e) => setFormData({...formData, email: e.target.value})}
                                className="w-full"
                            />
                        </div>
                        <div>
                            {/* 3. Added Flex Container for Label + Forgot Password Link */}
                            <div className="flex items-center justify-between mb-2">
                                <label className="block text-sm font-medium text-gray-700">Password</label>
                                <CustomLink 
                                    href="/admin/forgot-password"
                                    onClick={(e) => {
                                        e.preventDefault();
                                        navigate('/admin/forgot-password');
                                    }}
                                    className="text-sm font-semibold text-blue-600 hover:text-blue-800 cursor-pointer"
                                >
                                    Forgot Password?
                                </CustomLink>
                            </div>

                            <Password 
                                value={formData.password}
                                onChange={(e) => setFormData({...formData, password: e.target.value})}
                                className="w-full" inputClassName="w-full" feedback={false} toggleMask
                            />
                        </div>

                        <PrimaryButton 
                            label="Access Dashboard" 
                            type="submit" 
                            className="w-full" 
                            loading={actionLoading} 
                        />

                        <div className="text-center pt-4">
                            <CustomLink href="/admin/signup" className="text-sm">Register new Admin</CustomLink>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    );
};

export default AdminLogin;