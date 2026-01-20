import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Password } from 'primereact/password';
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';
import RegistrationBackground from '@/assets/registration-background.png';
import { useAdminAuth } from '@/context/AdminAuthContext';

const AdminSignup = () => {
    const { register, actionLoading, error } = useAdminAuth();
    
    // Local state for form and validation
    const [validationError, setValidationError] = useState('');
    const [formData, setFormData] = useState({ 
        username: '', 
        email: '', 
        password: '',
        confirmPassword: ''
    });

    // --- HELPER: Email Validation (Format + Length) ---
    const isValidEmail = (email) => {
        // 1. Standard Regex for format (e.g., text@domain.com)
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        
        // 2. Check: Matches Regex AND is at least 6 characters long
        return email.length >= 16 && emailRegex.test(email);
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setValidationError('');

        // 1. Check Email Validity
        if (!isValidEmail(formData.email)) {
            setValidationError("Please enter a valid email address (minimum 6 characters).");
            return;
        }

        // 2. Check Password Length
        if (formData.password.length < 8) {
            setValidationError("Password must be at least 8 characters long.");
            return;
        }

        // 3. Check if passwords match
        if (formData.password !== formData.confirmPassword) {
            setValidationError("Passwords do not match");
            return;
        }

        // 4. Register
        await register(formData.username, formData.email, formData.password);
    };

    return (
        <div className="min-h-screen bg-gray-900 flex items-center justify-center p-4 relative overflow-hidden">
            
            {/* Background Image Overlay */}
            <div className="absolute inset-0 z-0 opacity-20">
                <img src={RegistrationBackground} alt="Background" className="h-full w-full object-cover" />
            </div>

            <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8 sm:p-10 z-10">
                
                {/* Header Section */}
                <div className="mb-8 text-center">
                    <div className="inline-block px-3 py-1 bg-gray-100 rounded-full text-xs font-bold text-gray-600 mb-4 tracking-wider uppercase">
                        Internal Access Only
                    </div>
                    <Heading level={2} gradient={true} className="text-3xl mb-2 text-center">
                        Register Admin
                    </Heading>
                    <Text className="text-base text-center text-gray-500">
                        Create a new privileged account
                    </Text>
                </div>
                
                {/* Error Messages */}
                {(error || validationError) && (
                    <div className="mb-6 p-3 bg-red-50 text-red-600 rounded-lg text-sm text-center border border-red-100 flex items-center justify-center gap-2">
                        <i className="pi pi-exclamation-circle"></i>
                        <span>{validationError || error}</span>
                    </div>
                )}
                
                <form onSubmit={handleSubmit} className="space-y-5">
                    
                    {/* Username */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5 ml-1">Username</label>
                        <InputText 
                            placeholder="e.g. AdminUser" 
                            value={formData.username} 
                            onChange={(e) => setFormData({...formData, username: e.target.value})} 
                            className="w-full"
                            required 
                        />
                    </div>

                    {/* Email */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5 ml-1">Email Address</label>
                        <InputText 
                            type="email"
                            placeholder="admin@pulse.com" 
                            value={formData.email} 
                            onChange={(e) => setFormData({...formData, email: e.target.value})} 
                            // Highlights red if invalid format OR too short
                            className={`w-full ${validationError && !isValidEmail(formData.email) ? 'p-invalid' : ''}`}
                            required 
                        />
                    </div>

                    {/* Password */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5 ml-1">Password</label>
                        <Password 
                            placeholder="Create a strong password" 
                            value={formData.password} 
                            onChange={(e) => setFormData({...formData, password: e.target.value})} 
                            className="w-full" 
                            inputClassName="w-full" 
                            toggleMask 
                            required 
                        />
                    </div>

                    {/* Confirm Password */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1.5 ml-1">Confirm Password</label>
                        <Password 
                            placeholder="Repeat password" 
                            value={formData.confirmPassword} 
                            onChange={(e) => setFormData({...formData, confirmPassword: e.target.value})} 
                            className="w-full" 
                            inputClassName="w-full" 
                            feedback={false} 
                            toggleMask 
                            required 
                        />
                    </div>
                    
                    <PrimaryButton 
                        label="Create Account" 
                        type="submit" 
                        className="w-full mt-2" 
                        loading={actionLoading} 
                    />
                    
                    <div className="text-center pt-2">
                        <Text className="text-sm inline text-gray-500 mr-2">Already have an account?</Text>
                        <CustomLink href="/admin/login" className="text-sm font-semibold">
                            Back to Login
                        </CustomLink>
                    </div>
                </form>
            </div>
        </div>
    );
};

export default AdminSignup;