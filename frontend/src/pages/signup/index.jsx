import { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Password } from 'primereact/password';
import { Checkbox } from 'primereact/checkbox';
import { PrimaryButton } from '@/components/global/Button';
import { Heading, Text, CustomLink } from '@/components/global/Typography';

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
      {/* Left Section - Hidden on mobile/tablet */}
      <div className="hidden lg:flex lg:w-1/2 bg-gradient-to-br from-[var(--color-g1)] to-[var(--color-g2)] p-12 flex-col justify-between">
        <div>
          <Heading level={2} white={true} className="text-4xl mb-4">
            Pulse Analytics
          </Heading>
          <Text className="text-white text-lg opacity-90 mb-8">
            Transform your e-commerce data into actionable insights
          </Text>
        </div>

        <div className="space-y-6">
          <div className="flex items-start gap-4">
            <div className="w-12 h-12 rounded-full bg-white/20 flex items-center justify-center flex-shrink-0">
              <i className="pi pi-chart-line text-white text-xl"></i>
            </div>
            <div>
              <h3 className="text-white font-semibold text-lg mb-2">Real-time Analytics</h3>
              <p className="text-white/80 text-sm">Monitor your business metrics in real-time with powerful dashboards</p>
            </div>
          </div>

          <div className="flex items-start gap-4">
            <div className="w-12 h-12 rounded-full bg-white/20 flex items-center justify-center flex-shrink-0">
              <i className="pi pi-users text-white text-xl"></i>
            </div>
            <div>
              <h3 className="text-white font-semibold text-lg mb-2">Customer Insights</h3>
              <p className="text-white/80 text-sm">Understand your customers better with detailed behavioral analysis</p>
            </div>
          </div>

          <div className="flex items-start gap-4">
            <div className="w-12 h-12 rounded-full bg-white/20 flex items-center justify-center flex-shrink-0">
              <i className="pi pi-bolt text-white text-xl"></i>
            </div>
            <div>
              <h3 className="text-white font-semibold text-lg mb-2">Fast Integration</h3>
              <p className="text-white/80 text-sm">Get started quickly with seamless integration to your existing systems</p>
            </div>
          </div>
        </div>

        <div className="text-white/70 text-sm">
          © 2025 Pulse Analytics. All rights reserved.
        </div>
      </div>

      {/* Right Section - Form */}
      <div className="w-full lg:w-1/2 flex items-center justify-center p-6 sm:p-8 lg:p-12">
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
              <div>
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
              <div className="flex items-start gap-3">
                <Checkbox
                  inputId="agreeToTerms"
                  name="agreeToTerms"
                  checked={formData.agreeToTerms}
                  onChange={handleCheckboxChange}
                  required
                  className="mt-1"
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
                className="w-full py-3 text-base font-semibold"
                disabled={!formData.agreeToTerms}
              />

              {/* Sign In Link */}
              <div className="text-center pt-4">
                <Text className="text-sm inline">
                  Already have an account?{' '}
                </Text>
                <CustomLink className="text-sm font-semibold cursor-pointer">
                  Sign In
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

export default Signup;
