import React, { useState, useEffect } from 'react';
import { InputText } from 'primereact/inputtext';
import { Dropdown } from 'primereact/dropdown';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import { useNavigate } from 'react-router';
import { AutoComplete } from 'primereact/autocomplete';
import axiosInstance from '@/services/api/axiosInstance';
import { useAuth } from '@/context/AuthContext';
import { useLocation } from 'react-router-dom';
import usePageTitle from '@/hooks/usePageTitle';

const AddBusiness = () => {
    usePageTitle('Onboarding - Business Info');
    const navigate = useNavigate();
    const [businessName, setBusinessName] = useState('');
    const [currency, setCurrency] = useState(null);
    const [region, setRegion] = useState(null);
    const [loading, setLoading] = useState(false);

    const { pathname } = useLocation();

    const { user } = useAuth();

    const [errors, setErrors] = useState({form: '', businessName: '', businessCurrency: '', businessRegion: ''});

    const [currencySuggestions, setCurrencySuggestions] = useState([]);
    const [regionSuggestions, setRegionSuggestions] = useState([]);

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

    const fetchCurrentStep = async () => {
        try {
            const response = await axiosInstance.get(`/onboarding/get-current-step?userId=${user.user_id}`);
            const currentStep = response.data.currentStep;

            if (currentStep === 'business') {
                return;
            }
            else if (currentStep === 'data-type') {
                navigate(`/onboarding/data-type/${pathname.split('/')[3]}`);
            }
            else if (currentStep === 'connect') {
                navigate(`/onboarding/connect/${pathname.split('/')[3]}`);
            }
            else if (currentStep === 'mapping') {
                navigate(`/onboarding/mapping/${pathname.split('/')[3]}`);
            }
            else {
                navigate(`/onboarding/business/${pathname.split('/')[3]}`);
            }
        }

        catch (e) {
            setErrors((prev) => ({ ...prev, form: e.message || 'An error occurred while fetching onboarding status. Please try again.' }));
        }
    }

    useEffect(() => {
        fetchCurrentStep();
    }, []);

    const handleContinue = async (e) => {
        e.preventDefault();
        setErrors({form: '', businessName: '', businessCurrency: '', businessRegion: ''});
        setLoading(true);
        const nameRegex = /^[A-Za-z\s]+$/;

        if (!businessName) {
            setErrors((prev) => ({ ...prev, businessName: 'Business Name is required' }));
            setLoading(false);
            return;
        }

        if (!nameRegex.test(businessName)) {
            setErrors((prev) => ({
                ...prev,
                businessName: 'Business Name must contain only letters and spaces (no numbers or special characters)'
            }));
            setLoading(false);
            return;
        }

        if (!currency) {
            setErrors((prev) => ({ ...prev, currency: 'Currency is required' }));
            setLoading(false);
            return;
        }

        if (!region) {
            setErrors((prev) => ({ ...prev, region: 'Region is required' }));
            setLoading(false);
            return;
        }

        try {
            await axiosInstance.post('/onboarding/create-business', {
                userId: user.user_id,
                businessName,
                businessCurrency: currency,
                businessRegion: region,
            });
            navigate(`/onboarding/data-type/${pathname.split('/')[3]}`);
        }
        catch (e) {
            setErrors((prev) => ({ ...prev, form: e.message || 'An error occurred. Please try again.' }));
        }
        finally {
            setLoading(false);
        }
    };


    const isFormValid = businessName && currency && region;

    return (
        <div className="min-h-screen bg-gray-50 p-4 md:p-6 lg:p-8">
            {/* Breadcrumb and Step Indicator */}
            <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start gap-4">
                <Breadcrumb items={breadcrumbItems} />
                <Text className="text-sm text-gray-500 m-0 font-medium w-24 mt-4">
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
                    <form onSubmit={handleContinue} className="space-y-6 w-full">
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
                                aria-describedby="businessName-error"
                                invalid={errors.businessName ? true : false}
                            />
                            {errors.businessName && (
                                <p className="text-red-600 text-sm">{errors.businessName}</p>
                            )}
                        </div>
                        {/* Currency and Region Row */}
                        <div className="flex flex-col md:flex-row md:space-x-6 space-y-6 md:space-y-0 w-full">
                            {/* Currency */}
                            <div className="space-y-2 w-1/2">
                                <label
                                    htmlFor="currency"
                                    className="block text-sm font-medium text-[var(--color-text-primary)]"
                                >
                                    Currency
                                </label>
                                <AutoComplete
                                    id="currency"
                                    value={currency}
                                    onChange={(e) => setCurrency(e.value)}
                                    suggestions={currencySuggestions}
                                    completeMethod={async (e) => {
                                        const res = await axiosInstance.get(`http://localhost:8000/onboarding/api/currencies?query=${e.query}`);
                                        setCurrencySuggestions(res.data);
                                    }}
                                    placeholder="Currency"
                                    className="w-full p-invalid"
                                    disabled={loading}
                                    field="label"
                                    inputClassName={errors.businessCurrency ? 'p-invalid' : ''}
                                />
                                {errors.businessCurrency && (
                                    <p className="text-red-600 text-sm">{errors.businessCurrency}</p>
                                )}
                            </div>

                            {/* Region */}
                            <div className="space-y-2 w-1/2">
                                <label
                                    htmlFor="region"
                                    className="block text-sm font-medium text-[var(--color-text-primary)]"
                                >
                                    Region
                                </label>
                                <AutoComplete
                                    id="region"
                                    value={region}
                                    onChange={(e) => setRegion(e.value)}
                                    suggestions={regionSuggestions}
                                    completeMethod={async (e) => {
                                        const res = await axiosInstance.get(`http://localhost:8000/onboarding/api/regions?query=${e.query}`);
                                        setRegionSuggestions(res.data);
                                    }}
                                    placeholder="Country/Region"
                                    className="w-full"
                                    disabled={loading}
                                    field="label"
                                    inputClassName={errors.businessRegion ? 'p-invalid' : ''}
                                />
                                {errors.businessRegion && (
                                    <p className="text-red-600 text-sm">{errors.businessRegion}</p>
                                )}
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
                        {errors.form && (
                            <p className="text-red-600 text-sm text-center">{errors.form}</p>
                        )}
                    </form>
                </div>
            </div>
        </div>
    );
};

export default AddBusiness;