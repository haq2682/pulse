import React, { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router';
import { InputText } from 'primereact/inputtext';
import { FileUpload } from 'primereact/fileupload';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import axiosInstance from '@/services/api/axiosInstance';
import { useAuth } from '@/context/AuthContext';

const Connect = () => {
    const navigate = useNavigate();
    const [databaseUri, setDatabaseUri] = useState('');
    const [apiEndpoint, setApiEndpoint] = useState('');
    const { user } = useAuth();
    const [uploadedFiles, setUploadedFiles] = useState([{ name: 'products.csv' }, { name: 'sales_data.xlsx' }, { name: 'customers.parquet' }]);
    const [loading, setLoading] = useState(false);
    const [ingestionTypeLoading, setIngestionTypeLoading] = useState(true);
    const [ingestionType, setIngestionType] = useState('');
    const [errors, setErrors] = useState({db: 'Test Error', api: '', form: ''});
    const fileUploadRef = useRef(null);

    const breadcrumbItems = [
        {
            label: 'Business',
            active: false,
            clickable: false,
        },
        {
            label: 'Data Type',
            active: false,
            clickable: false,
        },
        {
            label: 'Connect',
            active: true,
            clickable: false
        },
        {
            label: 'Map',
            active: false,
            clickable: false
        },
    ];

    const handleFileSelect = (e) => {
        const files = Array.from(e.files);
        setUploadedFiles(files);
    };

    const handleFileRemove = (file) => {
        setUploadedFiles(uploadedFiles.filter(f => f !== file));
    };

    const handleContinue = async (e) => {
        e.preventDefault();
        setLoading(true);
        setErrors({});

        const apiRegex = /^(https?:\/\/)([a-zA-Z0-9.-]+)(:\d+)?(\/.*)?$/;
        const dbRegex = /^postgres:\/\/([^:@\s]+):([^:@\s]+)@([a-zA-Z0-9.-]+)(:\d+)?\/([a-zA-Z0-9_-]+)$/;

        if (ingestionType === 'batch') {
            if (uploadedFiles.length === 0) {
                setErrors({ form: "Please upload at least one file." });
                setLoading(false);
                return;
            }
        }

        if (ingestionType === 'db') {
            if (databaseUri.trim() === '') {
                setErrors({ db: "Please enter your Database URI." });
                setLoading(false);
                return;
            }

            if (!dbRegex.test(databaseUri)) {
                setErrors({ db: "Invalid Database URI format." });
                setLoading(false);
                return;
            }
        }

        if (ingestionType === 'api') {
            if (apiEndpoint.trim() === '') {
                setErrors({ api: "Please enter your API Endpoint." });
                setLoading(false);
                return;
            }

            if (!apiRegex.test(apiEndpoint)) {
                setErrors({ api: "Invalid API Endpoint." });
                setLoading(false);
                return;
            }
        }
    };


    const fetchIngestionType = async () => {
        setIngestionTypeLoading(true);
        try {
            const response = await axiosInstance.get('/onboarding/get-data-type', { params: { userId: user.user_id } });
            if (response.status === 200) {
                setIngestionType(response.data.dataType);
            }
        }
        catch (e) {
            console.log(e);
            setErrors((prev) => ({ ...prev, form: e.message || 'Failed to fetch ingestion type' }) );
        }
        finally {
            setIngestionTypeLoading(false);
        }
    }

    useEffect(() => {
        fetchIngestionType();
    }, []);

    const isFormValid = uploadedFiles.length > 0 || databaseUri.trim() !== '' || apiEndpoint.trim() !== '';

    return (
        <div className="min-h-screen bg-gray-50 p-4 md:p-6 lg:p-8">
            {/* Breadcrumb and Step Indicator */}
            <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start gap-4">
                <Breadcrumb items={breadcrumbItems} />
                <Text className="text-sm text-gray-500 m-0 font-medium w-24 mt-4">
                    Step 3 of 4
                </Text>
            </div>

            {/* Main Card */}
            <div className="max-w-6xl mx-auto">
                <div className="bg-white rounded-2xl shadow-lg p-6 md:p-8 lg:p-10">
                    {
                        ingestionTypeLoading ? (
                            <div className="mb-6">
                                <Text className="text-base text-gray-600 m-0 text-center">
                                    Loading...
                                </Text>
                            </div>
                        ) : ( 
                            <>                        
                                {/* Header */}
                                <div className="mb-6">
                                    <Heading level={2} gradient={false} className="text-2xl md:text-3xl mb-2">
                                        Connect your data
                                    </Heading>
                                    <Text className="text-sm md:text-base text-gray-600 m-0">
                                        Follow the guidelines and submit to validate your data.
                                    </Text>
                                </div>

                                {/* Guidelines Section */}
                                <div className="mb-8 bg-gray-50 rounded-lg p-6">
                                    <Heading level={3} black={true} className="text-lg font-bold mb-4" gradient={false}>
                                        Guidelines:
                                    </Heading>
                                    <ul className="space-y-3">
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • The data you provide will determine the scope of analyses, forecasts, and predictions generated by the system. You can upload any finite number of files.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • A set of generalized data attributes is defined for the system.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • If your dataset contains all of these attributes, regardless of their exact names, the system will perform all analyses it is programmed for.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • The system will first attempt to automatically identify the names of data attributes.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • If a specific data field is not detected, the system will request its mapping in the next section.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • If you skip the mapping of any field, the system will proceed with analytics, insights, and forecasts based on the available data attributes.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • Any analyses or forecasts requiring unmapped or missing attributes will be omitted.
                                            </Text>
                                        </li>
                                        <li className="flex items-start gap-2">
                                            <Text className="text-sm text-gray-700 m-0 flex-1">
                                                • The generalized data attributes can be downloaded from{' '}
                                                <CustomLink href="#" gradient={true}>
                                                    here
                                                </CustomLink>
                                            </Text>
                                        </li>
                                    </ul>
                                </div>

                                {/* Form */}
                                <form onSubmit={handleContinue} className="space-y-6">
                                    {/* File Upload Section */}
                                    {
                                        ingestionType === 'batch' && (
                                            <>
                                                <div className="space-y-4">
                                                    <div
                                                        className="border-2 border-dashed border-gray-300 rounded-xl p-8 md:p-12 text-center hover:border-[var(--color-g2)] transition-colors cursor-pointer"
                                                        onClick={() => fileUploadRef.current?.choose()}
                                                    >
                                                        <FileUpload
                                                            ref={fileUploadRef}
                                                            mode="advanced"
                                                            multiple
                                                            accept=".csv,.xlsx,.xls,.parquet"
                                                            maxFileSize={50000000}
                                                            onSelect={handleFileSelect}
                                                            chooseLabel="Choose Files"
                                                            uploadLabel="Upload"
                                                            cancelLabel="Cancel"
                                                            customUpload
                                                            auto={false}
                                                            className="hidden"
                                                        />
                                                        <div className="flex flex-col items-center gap-3">
                                                            <i className="pi pi-cloud-upload text-4xl text-gray-400"></i>
                                                            <Text className="text-gray-500 m-0">
                                                                Click Here or Drag Files Here to Upload
                                                            </Text>
                                                        </div>
                                                    </div>
                                                    {/* Uploaded Files Display - Above Database URI */}
                                                    {uploadedFiles.length > 0 && (
                                                        <div className="flex flex-wrap gap-6 py-4">
                                                            {uploadedFiles.map((file, index) => (
                                                                <div
                                                                    key={index}
                                                                    className="flex flex-col items-center gap-2 relative group"
                                                                >
                                                                    <button
                                                                        type="button"
                                                                        onClick={() => handleFileRemove(file)}
                                                                        className="absolute -top-2 -right-2 bg-red-500 text-white rounded-full w-5 h-5 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity z-10"
                                                                        title="Remove file"
                                                                    >
                                                                        <i className="pi pi-times text-xs"></i>
                                                                    </button>
                                                                    <div className="relative">
                                                                        <i className="pi pi-file text-3xl text-[var(--color-g2)]"></i>
                                                                    </div>
                                                                    <Text className="text-sm text-gray-700 m-0 text-center max-w-[120px] truncate">
                                                                        {file.name}
                                                                    </Text>
                                                                </div>
                                                            ))}
                                                        </div>
                                                    )}
                                                </div>
                                            </>
                                        )
                                    }
                                    {
                                        (ingestionType === 'db' || ingestionType === 'api') && (
                                            <>
                                                {/* Database/API URI */}
                                                <div className="space-y-2">
                                                    <label
                                                        htmlFor="databaseUri"
                                                        className="block text-sm font-medium text-[var(--color-text-primary)]"
                                                    >
                                                        {ingestionType === 'db' ? 'Database URI' : 'API Endpoint'}
                                                    </label>
                                                    {
                                                        ingestionType === 'db' ? (
                                                            <>
                                                                <InputText
                                                                    id="databaseUri"
                                                                    value={databaseUri}
                                                                    onChange={(e) => setDatabaseUri(e.target.value)}
                                                                    placeholder="Enter your Database URI here"
                                                                    className="w-full"
                                                                    disabled={loading}
                                                                    invalid={errors.db ? true : false}
                                                                />
                                                                {errors.db && (<>
                                                                    <Text className="text-red-500 text-sm">
                                                                        {errors.db}
                                                                    </Text>
                                                                </>)}
                                                                <Text className="text-gray-500 text-sm">
                                                                    URI Format: <Text className="font-mono">postgres://username:password@host:port/database</Text><br />
                                                                    <Text className="text-xs">Port is optional.</Text>
                                                                </Text>
                                                            </>
                                                        ) : (
                                                            <>
                                                                <InputText
                                                                    id="apiEndpoint"
                                                                    value={apiEndpoint}
                                                                    onChange={(e) => setApiEndpoint(e.target.value)}
                                                                    placeholder="Enter your API Endpoint here"
                                                                    className="w-full"
                                                                    disabled={loading}
                                                                    invalid={errors.api ? true : false}
                                                                />
                                                                {errors.api && (<>
                                                                    <Text className="text-red-500 text-sm">
                                                                        {errors.api}
                                                                    </Text>
                                                                </>)}
                                                                <Text className="text-gray-500 text-sm">
                                                                    API Format: <Text className="font-mono">https://api.example.com:port/path</Text><br />
                                                                    <Text className="text-xs">Port and path are optional. "http" or "https" are required.</Text>
                                                                </Text>
                                                            </>
                                                        )
                                                    }
                                                </div>
                                            </>
                                        )
                                    }
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
                                        <Text className="text-red-500 text-sm mt-2 text-center">
                                            {errors.form}
                                        </Text>
                                    )}
                                </form>
                            </>
                        )
                    }
                </div>
            </div>
        </div>
    );
};

export default Connect;