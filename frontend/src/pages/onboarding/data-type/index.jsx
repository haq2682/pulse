import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router';
import { useLocation } from 'react-router-dom';
import { RadioButton } from 'primereact/radiobutton';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import { useAuth } from '@/context/AuthContext';
import axiosInstance from '@/services/api/axiosInstance';
import { Message } from 'primereact/message';
import usePageTitle from '@/hooks/usePageTitle';

const DataType = () => {
    usePageTitle('Onboarding - Data Type');
    const navigate = useNavigate();
    const [selectedDataSource, setSelectedDataSource] = useState('batch');
    const { user } = useAuth();
    const [error, setError] = useState('');
    const { pathname } = useLocation();
    const [loading, setLoading] = useState(false);

    const breadcrumbItems = [
        {
            label: 'Business',
            active: false,
            clickable: false
        },
        {
            label: 'Data Type',
            active: true,
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

    const dataSourceOptions = [
        {
            id: 'batch',
            title: 'Files (CSV/Excel/Parquet/JSON)',
            description: 'Upload your E-Commerce Business data in bulk via files.',
            icon: 'pi-file'
        },
        {
            id: 'db',
            title: 'Database',
            description: 'Connect to a read-only schema or warehouse tables.',
            icon: 'pi-database'
        },
        {
            id: 'api',
            title: 'API',
            description: 'Pull data from your e-commerce or analytics APIs.',
            icon: 'pi-cloud'
        }
    ];

    const fetchCurrentStep = async () => {
        try {
            const response = await axiosInstance.get(`/onboarding/get-current-step?userId=${user.user_id}`);
            const currentStep = response.data.currentStep;

            if (currentStep === 'business') {
                navigate(`/onboarding/business/${pathname.split('/')[3]}`);
            }
            else if (currentStep === 'data-type') {
                return;
            }
            else if (currentStep === 'connect') {
                navigate(`/onboarding/connect/${pathname.split('/')[3]}`);
            }
            else if (currentStep === 'mapping') {
                navigate(`/onboarding/mapping/${pathname.split('/')[3]}`);
            }
            else {
                navigate(`/onboarding/data-type/${pathname.split('/')[3]}`);
            }
        }

        catch (e) {
            setError(e.message || 'An error occurred while fetching onboarding status. Please try again.');
        }
    }

    useEffect(() => {
        fetchCurrentStep();
    }, []);

    const handleContinue = async (e) => {
        e.preventDefault();
        setLoading(true);

        try {
            const response = await axiosInstance.post('/onboarding/select-data-type', {
                userId: user.user_id,
                dataType: selectedDataSource,
            });
            if(response.status === 200) {
                navigate(`/onboarding/connect/${pathname.split('/')[3]}`);
            }
        }
        catch (e) {
            setError(e.message || 'An unexpected error occurred. Please try again.');
        }
        finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 p-4 md:p-6 lg:p-8">
            {/* Breadcrumb and Step Indicator */}
            <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start gap-4">
                <Breadcrumb items={breadcrumbItems} />
                <Text className="text-sm text-gray-500 m-0 font-medium w-24 mt-4">
                    Step 2 of 4
                </Text>
            </div>

            {/* Main Card */}
            <div className="max-w-6xl mx-auto">
                <div className="bg-white rounded-2xl shadow-lg p-6 md:p-8 lg:p-10">
                    {/* Header */}
                    <div className="mb-8">
                        <Heading level={2} gradient={true} className="text-2xl md:text-3xl mb-2">
                            Select your data source
                        </Heading>
                        <Text className="text-sm md:text-base text-gray-600 m-0">
                            Bring data via files, a database connection, or a REST API.
                        </Text>
                        <Message
                            style={{
                                border: 'solid #00C597',
                                borderWidth: '0 0 0 6px',
                                color: '#00C597'
                            }}
                            className="w-full text-left flex justify-start items-start gap-3 mt-4"
                            severity="success"
                            content={
                                <>
                                    <div className="font-bold text-left my-4">
                                        <div className="ml-2">NOTE: Batch Processing can process files up to 5 GB. If you have even larger data, then consider either API or Database URI option</div>
                                    </div>
                                </>
                            }
                        />
                    </div>

                    {/* Data Source Options */}
                    <form onSubmit={handleContinue} className="space-y-4">
                        {dataSourceOptions.map((option) => (
                            <div
                                key={option.id}
                                onClick={() => setSelectedDataSource(option.id)}
                                className={`
                                    border-2 rounded-xl p-6 cursor-pointer transition-all duration-200
                                    ${selectedDataSource === option.id
                                        ? 'border-[var(--color-g2)] bg-gradient-to-r from-[rgba(0,197,151,0.02)] to-[rgba(18,239,188,0.02)]'
                                        : 'border-gray-200 hover:border-gray-300 bg-white'
                                    }
                                `}
                            >
                                <div className="flex items-start gap-4">
                                    {/* Radio Button */}
                                    <div className="pt-1">
                                        <RadioButton
                                            inputId={option.id}
                                            name="dataSource"
                                            value={option.id}
                                            onChange={(e) => setSelectedDataSource(e.value)}
                                            checked={selectedDataSource === option.id}
                                            className="custom-radio"
                                        />
                                    </div>

                                    {/* Content */}
                                    <div className="flex-1">
                                        <Heading
                                            level={3}
                                            gradient={selectedDataSource === option.id}
                                            className="text-lg md:text-xl mb-1"
                                        >
                                            {option.title}
                                        </Heading>
                                        <Text className="text-sm md:text-base text-gray-600 m-0">
                                            {option.description}
                                        </Text>
                                    </div>
                                </div>
                            </div>
                        ))}

                        {/* Continue Button */}
                        <div className="flex justify-end pt-6">
                            <PrimaryButton
                                label="Continue"
                                onClick={handleContinue}
                                loading={loading}
                                disabled={loading}
                                className="px-8"
                            />
                        </div>
                        {error && (
                            <div className="mt-4 text-red-600 text-center">
                                {error}
                            </div>
                        )}
                    </form>
                </div>
            </div>
        </div>
    );
};

export default DataType;