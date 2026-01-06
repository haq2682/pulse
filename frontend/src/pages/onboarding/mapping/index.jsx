import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router';
import { Dropdown } from 'primereact/dropdown';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';

const Mapping = () => {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);
    const [dataLoading, setDataLoading] = useState(true);

    // Dynamic state for mappings
    const [mappings, setMappings] = useState({});
    const [requiredFields, setRequiredFields] = useState([]);
    const [availableColumns, setAvailableColumns] = useState([]);
    const [allFieldsIdentified, setAllFieldsIdentified] = useState(false);

    const breadcrumbItems = [
        {
            label: 'Business',
            active: false,
            clickable: true,
            onClick: () => navigate('/onboarding/business')
        },
        {
            label: 'Data Type',
            active: false,
            clickable: true,
            onClick: () => navigate('/onboarding/data-type')
        },
        {
            label: 'Connect',
            active: false,
            clickable: true,
            onClick: () => navigate('/onboarding/connect')
        },
        {
            label: 'Map',
            active: true,
            clickable: false
        },
    ];

    const fetchMappingData = async () => {
        setDataLoading(true);

        // Mock API call - Replace with actual backend call
        setTimeout(() => {
            // Mock response from backend
            const mockResponse = {
                allFieldsIdentified: true, // or false if some fields need mapping
                requiredFields: [
                    { key: 'billing_address', label: 'Billing Address', identified: false },
                    { key: 'product_name', label: 'Product Name', identified: false },
                    { key: 'cost_price', label: 'Cost Price', identified: false },
                    { key: 'payment_id', label: 'Payment ID', identified: false },
                    { key: 'category_name', label: 'Category Name', identified: false },
                    { key: 'postal_code', label: 'Postal Code', identified: false },
                ],
                availableColumns: [
                    'customer_address',
                    'product_title',
                    'price',
                    'transaction_id',
                    'category',
                    'zip_code',
                    'order_date',
                    'customer_name',
                    'quantity',
                    'product_description',
                    'customer_email',
                ]
            };

            setAllFieldsIdentified(mockResponse.allFieldsIdentified);
            setRequiredFields(mockResponse.requiredFields.filter(field => !field.identified));
            setAvailableColumns(mockResponse.availableColumns);

            // Initialize mappings object with null values
            const initialMappings = {};
            mockResponse.requiredFields
                .filter(field => !field.identified)
                .forEach(field => {
                    initialMappings[field.key] = null;
                });
            setMappings(initialMappings);

            setDataLoading(false);
        }, 1000);
    };

    useEffect(() => {
        // TODO: Replace with actual API call
        const fetchData = async () => {
            await fetchMappingData();
        };
        fetchData();
    }, []);

    const handleMappingChange = (fieldKey, value) => {
        setMappings(prev => ({
            ...prev,
            [fieldKey]: value
        }));
    };

    const handleContinue = async (e) => {
        e.preventDefault();
        setLoading(true);

        // TODO: Add your API call here to save mappings
        const mappingData = {
            mappings: mappings,
            skippedFields: Object.keys(mappings).filter(key => mappings[key] === null)
        };

        console.log('Submitting mappings:', mappingData);

        setTimeout(() => {
            setLoading(false);
            // Navigate to dashboard
            navigate('/dashboard/overview');
        }, 1500);
    };

    // Convert available columns to dropdown options
    const columnOptions = availableColumns.map(col => ({
        label: col,
        value: col
    }));

    if (dataLoading) {
        return (
            <div className="min-h-screen bg-gray-50 flex items-center justify-center">
                <div className="text-center">
                    <i className="pi pi-spin pi-spinner text-4xl text-[var(--color-g2)] mb-4"></i>
                    <Text className="text-gray-600">Loading mapping data...</Text>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-gray-50 p-4 md:p-6 lg:p-8">
            {/* Breadcrumb and Step Indicator */}
            <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
                <Breadcrumb items={breadcrumbItems} />
                <Text className="text-sm text-gray-500 m-0 font-medium">
                    Step 4 of 4
                </Text>
            </div>

            {/* Main Card */}
            <div className="max-w-6xl mx-auto">
                <div className="bg-white rounded-2xl shadow-lg p-6 md:p-8 lg:p-10">
                    {/* Header */}
                    <div className="mb-6">
                        <Heading level={2} gradient={true} className="text-2xl md:text-3xl mb-2">
                            Mapping the Data
                        </Heading>
                        <Text className="text-sm md:text-base text-gray-600 m-0">
                            Map the data fields that the system was unable to identify
                        </Text>
                    </div>

                    {/* Success Message - Show if all fields identified */}
                    {allFieldsIdentified && requiredFields.length === 0 && (
                        <div className="mb-8 py-6 border-y border-gray-200">
                            <Heading level={3} gradient={true} className="text-lg md:text-xl text-center m-0">
                                The system identified all the required data fields. You do not need to map anything manually.
                            </Heading>
                        </div>
                    )}

                    {/* Fields to Map Message - Show if some fields need mapping */}
                    {requiredFields.length > 0 && (
                        <div className="mb-6 py-4 border-y border-gray-200">
                            <Text className="text-center text-gray-700 m-0">
                                The following fields could not be automatically identified. Please map them to your data columns:
                            </Text>
                        </div>
                    )}

                    {/* Note */}
                    <div className="mb-8 bg-green-50 border-l-4 border-[var(--color-g2)] p-4 rounded">
                        <Text className="text-sm text-gray-700 m-0">
                            <span className="font-bold text-[var(--color-g1)]">Note:</span> If you skip providing any required data field from the drop-down list(s), then the system will automatically omit the analysis which requires that particular field(s).
                        </Text>
                    </div>

                    {/* Form */}
                    <form onSubmit={handleContinue} className="space-y-6">
                        {/* Dynamic Mapping Fields Grid */}
                        {requiredFields.length > 0 && (
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-6">
                                {requiredFields.map((field) => (
                                    <div key={field.key} className="flex items-center gap-4">
                                        <label
                                            htmlFor={field.key}
                                            className="text-sm font-semibold text-gray-900 min-w-[140px] text-right"
                                        >
                                            {field.label}:
                                        </label>
                                        <Dropdown
                                            id={field.key}
                                            value={mappings[field.key]}
                                            onChange={(e) => handleMappingChange(field.key, e.value)}
                                            options={columnOptions}
                                            placeholder="Select an option"
                                            className="flex-1"
                                            disabled={loading}
                                            showClear
                                        />
                                    </div>
                                ))}
                            </div>
                        )}

                        {/* No Fields to Map Message */}
                        {requiredFields.length === 0 && allFieldsIdentified && (
                            <div className="text-center py-8">
                                <i className="pi pi-check-circle text-6xl text-[var(--color-g2)] mb-4"></i>
                                <Text className="text-gray-600 text-lg">
                                    All data fields have been successfully identified!
                                </Text>
                            </div>
                        )}

                        {/* Continue Button */}
                        <div className="flex justify-end pt-6">
                            <PrimaryButton
                                label="Continue to Dashboard"
                                onClick={handleContinue}
                                loading={loading}
                                disabled={loading}
                                className="px-8"
                            />
                        </div>
                    </form>
                </div>
            </div>
        </div>
    );
};

export default Mapping;