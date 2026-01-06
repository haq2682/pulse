import React, { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { Dropdown } from 'primereact/dropdown';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import { useNavigate } from 'react-router';

const AddBusiness = () => {
    const navigate = useNavigate();
    const [businessName, setBusinessName] = useState('');
    const [currency, setCurrency] = useState(null);
    const [region, setRegion] = useState(null);
    const [loading, setLoading] = useState(false);

    const currencyOptions = [
        { label: 'USD', value: 'USD' },
        { label: 'EUR', value: 'EUR' },
        { label: 'AUD', value: 'AUD' },
        { label: 'GBP', value: 'GBP' },
        { label: 'CAD', value: 'CAD' },
        { label: 'JPY', value: 'JPY' },
    ];

    const regionOptions = [
        { label: 'US', value: 'US' },
        { label: 'EU', value: 'EU' },
        { label: 'AU', value: 'AU' },
        { label: 'UK', value: 'UK' },
        { label: 'CA', value: 'CA' },
        { label: 'Asia', value: 'Asia' },
    ];

    const breadcrumbItems = [
        {
            label: 'Business',
            active: true,
            clickable: false
        },
        {
            label: 'Data Type',
            active: false,
            clickable: false
        },
        {
            label: 'Connect',
            active: false,
            clickable: false
        },
        {
            label: 'Map',
            active: false,
            clickable: false
        },
    ];

    const handleContinue = async (e) => {
        e.preventDefault();

        if (!businessName || !currency || !region) {
            // TODO: Show validation error
            return;
        }

        setLoading(true);

        // TODO: Add your API call here
        setTimeout(() => {
            setLoading(false);
            // Navigate to next step
            navigate('/onboarding/data-type');
        }, 1500);
    };

    const isFormValid = businessName && currency && region;

    return (
        <div className="min-h-screen bg-gray-50 p-4 md:p-6 lg:p-8">
            {/* Breadcrumb and Step Indicator */}
            <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
                <Breadcrumb items={breadcrumbItems} />
                <Text className="text-sm text-gray-500 m-0 font-medium">
                    Step 1 of 4
                </Text>
            </div>

            {/* Main Card */}
            <div className="max-w-6xl mx-auto">
                <div className="bg-white rounded-2xl shadow-lg p-6 md:p-8 lg:p-10">
                    {/* Header */}
                    <div className="mb-8">
                        <Heading level={2} gradient={true} className="text-2xl md:text-3xl mb-2">
                            Tell us about your business
                        </Heading>
                        <Text className="text-sm md:text-base text-gray-600 m-0">
                            We'll tailor defaults for currency, region, and KPIs.
                        </Text>
                    </div>

                    {/* Form */}
                    <form onSubmit={handleContinue} className="space-y-6">
                        {/* Business Name */}
                        <div className="space-y-2">
                            <label
                                htmlFor="businessName"
                                className="block text-sm font-medium text-[var(--color-text-primary)]"
                            >
                                Business Name
                            </label>
                            <InputText
                                id="businessName"
                                value={businessName}
                                onChange={(e) => setBusinessName(e.target.value)}
                                placeholder="Business Name"
                                required
                                className="w-full"
                                disabled={loading}
                            />
                        </div>

                        {/* Currency and Region Row */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
                            {/* Currency */}
                            <div className="space-y-2">
                                <label
                                    htmlFor="currency"
                                    className="block text-sm font-medium text-[var(--color-text-primary)]"
                                >
                                    Currency
                                </label>
                                <Dropdown
                                    id="currency"
                                    value={currency}
                                    onChange={(e) => setCurrency(e.value)}
                                    options={currencyOptions}
                                    placeholder="USD / EUR / AUD"
                                    className="w-full"
                                    disabled={loading}
                                />
                            </div>

                            {/* Region */}
                            <div className="space-y-2">
                                <label
                                    htmlFor="region"
                                    className="block text-sm font-medium text-[var(--color-text-primary)]"
                                >
                                    Region
                                </label>
                                <Dropdown
                                    id="region"
                                    value={region}
                                    onChange={(e) => setRegion(e.value)}
                                    options={regionOptions}
                                    placeholder="US / EU / AU"
                                    className="w-full"
                                    disabled={loading}
                                />
                            </div>
                        </div>

                        {/* Continue Button */}
                        <div className="flex justify-end pt-4">
                            <PrimaryButton
                                label="Continue"
                                onClick={handleContinue}
                                loading={loading}
                                disabled={loading || !isFormValid}
                                className="px-8"
                            />
                        </div>
                    </form>
                </div>
            </div>
        </div>
    );
};

export default AddBusiness;