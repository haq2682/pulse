import React, { useState, useRef, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router';
import { InputText } from 'primereact/inputtext';
import { ProgressBar } from 'primereact/progressbar';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import axiosInstance from '@/services/api/axiosInstance';
import { useAuth } from '@/context/AuthContext';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Dialog } from 'primereact/dialog';
import { Message } from 'primereact/message';
import { SecondaryButton } from '@/components/global/Button';
import usePageTitle from '@/hooks/usePageTitle';

const CHUNK_SIZE = 5 * 1024 * 1024;
const MAPPING_STATUS_POLL_INTERVAL = 3000; // 3 seconds
// const NIFI_UPLOAD_URL = import.meta.env.VITE_NIFI_UPLOAD_URL || 'http://10.5.0.12:8082/upload';
const NIFI_UPLOAD_URL = 'http://localhost:8082/upload';

const Connect = () => {
    usePageTitle('Onboarding - Connect');
    const navigate = useNavigate();
    const location = useLocation();
    const pathname = location.pathname;
    const [databaseUri, setDatabaseUri] = useState('');
    const [apiEndpoint, setApiEndpoint] = useState('');
    const [businessId, setBusinessId] = useState('');
    const { user } = useAuth();
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [uploadProgress, setUploadProgress] = useState({});
    const [loading, setLoading] = useState(false);
    const [mappingLoading, setMappingLoading] = useState(false);
    const [cancellingMapping, setCancellingMapping] = useState(false);
    const [ingestionTypeLoading, setIngestionTypeLoading] = useState(true);
    const [ingestionType, setIngestionType] = useState('');
    const [errors, setErrors] = useState({db: '', api: '', form: ''});
    const [isDragging, setIsDragging] = useState(false);
    const fileInputRef = useRef(null);
    const mappingCheckIntervalRef = useRef(null);
    const isCheckingMappingRef = useRef(false); // Prevent duplicate interval creation

    // Helper function to safely extract onboarding ID from pathname
    const getOnboardingIdFromPath = () => {
        const pathSegments = pathname.split('/');
        return pathSegments.length > 3 ? pathSegments[3] : '';
    };

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

    const fetchCurrentStep = async () => {
        try {
            const response = await axiosInstance.get(`/onboarding/get-current-step?userId=${user.user_id}`);
            const currentStep = response.data.currentStep;
            const onboardingId = getOnboardingIdFromPath();

            if (currentStep === 'business') {
                navigate(`/onboarding/business/${onboardingId}`);
            }
            else if (currentStep === 'data-type') {
                navigate(`/onboarding/data-type/${onboardingId}`);
            }
            else if (currentStep === 'connect' || currentStep === 'mapping-in-progress') {
                // Stay on connect page if mapping is in progress
                return;
            }
            else if (currentStep === 'mapping') {
                navigate(`/onboarding/mapping/${onboardingId}`);
            }
            else {
                navigate(`/onboarding/connect/${onboardingId}`);
            }
        }

        catch (e) {
            setErrors((prev) => ({
                ...prev,
                form: e.message || 'An error occurred while fetching onboarding status. Please try again.'
            }));
        }
    }

    useEffect(() => {
        if (!user?.user_id) return;
        
        const initializeData = async () => {
            await fetchCurrentStep();
            await fetchIngestionType();
            await fetchUploadedFiles();
            // Check mapping status after data is loaded
            await checkMappingStatus();
        };
        
        initializeData();
        
        // Clean up interval on unmount
        return () => {
            if (mappingCheckIntervalRef.current) {
                clearInterval(mappingCheckIntervalRef.current);
            }
        };
    }, [user?.user_id]);

    const fetchUploadedFiles = async () => {
        try {
            const response = await axiosInstance.get('/onboarding/uploaded-files', {
                params: { userId: user.user_id }
            });
            if (response.status === 200 && response.data.files) {
                const files = response.data.files.map(f => ({
                    fileId: f.fileId,
                    name: f.fileName,
                    size: f.fileSize,
                    type: f.fileType,
                    persisted: true
                }));
                setUploadedFiles(files);
            }
        } catch (e) {
            console.error('Error fetching uploaded files:', e);
        }
    };

    // const uploadFileInChunks = async (file, fileId) => {
    //     const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    //     try {
    //         for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
    //             const start = chunkIndex * CHUNK_SIZE;
    //             const end = Math.min(start + CHUNK_SIZE, file.size);
    //             const chunk = file.slice(start, end);

    //             const formData = new FormData();
    //             formData.append('chunk', chunk);
    //             formData.append('chunkIndex', chunkIndex);
    //             formData.append('totalChunks', totalChunks);
    //             formData.append('fileId', fileId);
    //             formData.append('fileName', file.name);
    //             formData.append('fileSize', file.size);
    //             formData.append('fileType', file.type);
    //             formData.append('userId', user.user_id);

    //             await axiosInstance.post('/onboarding/upload-chunk', formData, {
    //                 headers: { 'Content-Type': 'multipart/form-data' }
    //             });

    //             const progress = Math.round(((chunkIndex + 1) / totalChunks) * 100);
    //             setUploadProgress(prev => ({ ...prev, [fileId]: progress }));
    //         }

    //         setUploadedFiles(prev => prev.map(f => 
    //             f.fileId === fileId ? { ...f, persisted: true } : f
    //         ));

    //         setTimeout(() => {
    //             setUploadProgress(prev => {
    //                 const newProgress = { ...prev };
    //                 delete newProgress[fileId];
    //                 return newProgress;
    //             });
    //         }, 1000);

    //     } catch (error) {
    //         console.error('Upload error:', error);
    //         setUploadedFiles(prev => prev.filter(f => f.fileId !== fileId));
    //         setUploadProgress(prev => {
    //             const newProgress = { ...prev };
    //             delete newProgress[fileId];
    //             return newProgress;
    //         });
    //         throw error;
    //     }
    // };

    const uploadFileToNiFi = async (file, fileId) => {
        const formData = new FormData();
        formData.append('file', file);  // Only send the file

        try {
            const response = await axiosInstance.post(NIFI_UPLOAD_URL, formData, {
                headers: { 
                    'Content-Type': 'multipart/form-data',
                    'Accept': 'application/json',
                    // ✅ Send metadata as HTTP headers
                    'X-File-Id': fileId,
                    'X-User-Id': user.user_id,
                    'X-Business-Id': businessId
                },
                baseURL: '',
                withCredentials: false,
                onUploadProgress: (progressEvent) => {
                    if (progressEvent.total) {
                        const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
                        setUploadProgress(prev => ({ ...prev, [fileId]: progress }));
                    }
                },
                timeout: 1800000,
            });

            if (response.status === 200 || response.status === 202) {
                setUploadedFiles(prev => prev.map(f =>
                    f.fileId === fileId ? { ...f, persisted: true } : f
                ));

                setTimeout(() => {
                    setUploadProgress(prev => {
                        const newProgress = { ...prev };
                        delete newProgress[fileId];
                        return newProgress;
                    });
                }, 1000);

                return response.data;
            }
        } catch (error) {
            console.error('Upload error:', error);
            setUploadedFiles(prev => prev.filter(f => f.fileId !== fileId));
            setUploadProgress(prev => {
                const newProgress = { ...prev };
                delete newProgress[fileId];
                return newProgress;
            });
            throw new Error(error.response?.data?.message || error.message || 'Upload failed');
        }
    };
    const validateFile = (file) => {
        const allowedExtensions = ['csv', 'xlsx', 'xls', 'parquet', 'json'];
        const fileName = file.name.toLowerCase();
        const fileExtension = fileName.split('.').pop();
        return allowedExtensions.includes(fileExtension);
    };

    // const handleFileSelect = async (files) => {
    //     const fileArray = Array.isArray(files) ? files : Array.from(files);
        
    //     const validFiles = fileArray.filter(file => {
    //         if (!validateFile(file)) {
    //             console.warn(`File ${file.name} has invalid format`);
    //             return false;
    //         }
    //         return true;
    //     });

    //     if (validFiles.length === 0) {
    //         setErrors(prev => ({ ...prev, form: 'Please select valid file formats (CSV, XLSX, XLS, Parquet, JSON)' }));
    //         return;
    //     }

    //     const newFiles = validFiles.filter(file => {
    //         return !uploadedFiles.some(f => f.name === file.name && f.size === file.size);
    //     });

    //     for (const file of newFiles) {
    //         const fileId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}_${file.name}`;
    //         const fileObj = {
    //             fileId,
    //             name: file.name,
    //             size: file.size,
    //             type: file.type,
    //             persisted: false
    //         };
            
    //         setUploadedFiles(prev => [...prev, fileObj]);
    //         setUploadProgress(prev => ({ ...prev, [fileId]: 0 }));
            
    //         uploadFileInChunks(file, fileId).catch(err => {
    //             setErrors(prev => ({ ...prev, form: `Failed to upload ${file.name}` }));
    //         });
    //     }

    //     setErrors(prev => ({ ...prev, form: '' }));
        
    //     if (fileInputRef.current) {
    //         fileInputRef.current.value = '';
    //     }
    // };

    const handleFileSelect = async (files) => {
        const fileArray = Array.isArray(files) ? files : Array.from(files);

        const validFiles = fileArray.filter(file => {
            if (!validateFile(file)) {
                console.warn(`File ${file.name} has invalid format`);
                return false;
            }
            return true;
        });

        if (validFiles.length === 0) {
            setErrors(prev => ({ ...prev, form: 'Please select valid file formats (CSV, XLSX, XLS, Parquet, JSON)' }));
            return;
        }

        const newFiles = validFiles.filter(file => {
            return !uploadedFiles.some(f => f.name === file.name && f.size === file.size);
        });

        for (const file of newFiles) {
            const fileId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}_${file.name}`;
            const fileObj = {
                fileId,
                name: file.name,
                size: file.size,
                type: file.type,
                persisted: false
            };

            setUploadedFiles(prev => [...prev, fileObj]);
            setUploadProgress(prev => ({ ...prev, [fileId]: 0 }));

            // Use NiFi upload instead of chunked upload
            uploadFileToNiFi(file, fileId).catch(err => {
                setErrors(prev => ({ ...prev, form: `Failed to upload ${file.name}: ${err.message}` }));
            });
        }

        setErrors(prev => ({ ...prev, form: '' }));

        if (fileInputRef.current) {
            fileInputRef.current.value = '';
        }
    };

    const handleFileInputChange = (e) => {
        if (e.target.files && e.target.files.length > 0) {
            handleFileSelect(e.target.files);
        }
    };

    const handleDragOver = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setIsDragging(true);
    };

    const handleDragLeave = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setIsDragging(false);
    };

    const handleDrop = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setIsDragging(false);

        const files = e.dataTransfer.files;
        if (files && files.length > 0) {
            handleFileSelect(files);
        }
    };

    const handleClickUploadArea = () => {
        fileInputRef.current?.click();
    };

    const handleFileRemove = async (file) => {
        try {
            if (file.persisted) {
                await axiosInstance.delete('/onboarding/delete-file', {
                    data: { fileId: file.fileId, userId: user.user_id }
                });
            }
            
            setUploadedFiles(prev => prev.filter(f => f.fileId !== file.fileId));
            setUploadProgress(prev => {
                const newProgress = { ...prev };
                delete newProgress[file.fileId];
                return newProgress;
            });
        } catch (error) {
            console.error('Error removing file:', error);
            setErrors(prev => ({ ...prev, form: 'Failed to remove file' }));
        }
    };

    const handleContinue = async (e) => {
        e.preventDefault();
        setLoading(true);
        setErrors({db: '', api: '', form: ''});

        const apiRegex = /^(https?:\/\/)([a-zA-Z0-9.-]+)(:\d+)?(\/.*)?$/;
        const dbRegex = /^([a-zA-Z0-9]+):\/\/([^:@\s]+):([^:@\s]+)@([a-zA-Z0-9.-]+)(:\d+)?\/([a-zA-Z0-9_-]+)$/;

        if (ingestionType === 'batch') {
            if (uploadedFiles.length === 0) {
                setErrors({ form: "Please upload at least one file." });
                setLoading(false);
                return;
            }
            const uploading = Object.keys(uploadProgress).length > 0;
            if (uploading) {
                setErrors({ form: "Please wait for all files to finish uploading." });
                setLoading(false);
                return;
            }
            
            // Start the mapping pipeline for batch mode
            await startMapping();
            return;
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
            
            // Start the mapping pipeline for db mode
            await startMapping();
            return;
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

            // Start the mapping pipeline for api mode
            await startMapping();
            return;
        }
        
        setLoading(false);
    };

    const fetchIngestionType = async () => {
        setIngestionTypeLoading(true);
        try {
            const response = await axiosInstance.get('/onboarding/get-data-type', { params: { userId: user.user_id } });
            if (response.status === 200) {
                setIngestionType(response.data.dataType);
                setBusinessId(response.data.businessId || '');
            }
        }
        catch (e) {
            setErrors((prev) => ({ ...prev, form: e.message || 'Failed to fetch ingestion type' }) );
        }
        finally {
            setIngestionTypeLoading(false);
        }
    }

    const checkMappingStatus = async () => {
        // Prevent duplicate checks
        if (isCheckingMappingRef.current) {
            return;
        }
        
        try {
            isCheckingMappingRef.current = true;
            const response = await axiosInstance.get('/onboarding/mapping-status', {
                params: { userId: user.user_id }
            });
            
            if (response.status === 200) {
                const { current_step, mapping_status } = response.data;
                
                // If mapping is in progress, show the loader and start polling
                if (mapping_status === 'running' || current_step === 'mapping-in-progress') {
                    setMappingLoading(true);
                    
                    // Start polling every 3 seconds if not already polling
                    if (!mappingCheckIntervalRef.current) {
                        // Reset the flag after initial setup to allow future calls
                        isCheckingMappingRef.current = false;
                        
                        mappingCheckIntervalRef.current = setInterval(async () => {
                            try {
                                const statusResponse = await axiosInstance.get('/onboarding/mapping-status', {
                                    params: { userId: user.user_id }
                                });
                                
                                if (statusResponse.status === 200) {
                                    const { mapping_status: status } = statusResponse.data;
                                    
                                    // If mapping is completed or failed, stop polling and hide loader
                                    if (status === 'completed') {
                                        clearInterval(mappingCheckIntervalRef.current);
                                        mappingCheckIntervalRef.current = null;
                                        setMappingLoading(false);
                                        // Navigate to mapping page
                                        const onboardingId = getOnboardingIdFromPath();
                                        navigate(`/onboarding/mapping/${onboardingId}`);
                                    } else if (status === 'failed') {
                                        clearInterval(mappingCheckIntervalRef.current);
                                        mappingCheckIntervalRef.current = null;
                                        setMappingLoading(false);
                                        setErrors((prev) => ({ 
                                            ...prev, 
                                            form: statusResponse.data.mapping_error || 'Mapping failed. Please try again.' 
                                        }));
                                    }
                                }
                            } catch (err) {
                                console.error('Error checking mapping status:', err);
                                // Stop polling on error to avoid infinite polling with a stuck state
                                if (mappingCheckIntervalRef.current) {
                                    clearInterval(mappingCheckIntervalRef.current);
                                    mappingCheckIntervalRef.current = null;
                                }
                                setMappingLoading(false);
                                setErrors((prev) => ({
                                    ...prev,
                                    form: prev.form || 'An error occurred while checking mapping status. Please try again.'
                                }));
                            }
                        }, MAPPING_STATUS_POLL_INTERVAL);
                    } else {
                        // Interval already exists, just reset the flag
                        isCheckingMappingRef.current = false;
                    }
                } else if (mapping_status === 'completed') {
                    // If mapping is already completed, navigate to mapping page
                    isCheckingMappingRef.current = false;
                    const onboardingId = getOnboardingIdFromPath();
                    navigate(`/onboarding/mapping/${onboardingId}`);
                } else {
                    setMappingLoading(false);
                    isCheckingMappingRef.current = false;
                }
            }
        } catch (e) {
            console.error('Error checking mapping status:', e);
            isCheckingMappingRef.current = false;
        }
    };

    const startMapping = async () => {
        try {
            setLoading(true);
            setErrors({ db: '', api: '', form: '' });
            
            const requestBody = {
                userId: user.user_id,
                mode: ingestionType // Use the ingestion type as the mode
            };
            
            // Add mode-specific parameters
            if (ingestionType === 'db') {
                requestBody.dbUri = databaseUri;
                // You can add db_tables here if needed
                // requestBody.dbTables = ['table1', 'table2'];
            } else if (ingestionType === 'api') {
                requestBody.apiUrl = apiEndpoint;
            }
            
            const response = await axiosInstance.post('/onboarding/start-mapping', requestBody);
            
            if (response.status === 200) {
                setMappingLoading(true);
                setLoading(false);
                
                // Start polling for status
                checkMappingStatus();
            }
        } catch (e) {
            setLoading(false);
            const errorMessage = e.response?.data?.detail || e.message || 'Failed to start mapping pipeline';
            
            // Set error in appropriate field based on mode
            if (ingestionType === 'db') {
                setErrors((prev) => ({ ...prev, db: errorMessage }));
            } else if (ingestionType === 'api') {
                setErrors((prev) => ({ ...prev, api: errorMessage }));
            } else {
                setErrors((prev) => ({ ...prev, form: errorMessage }));
            }
        }
    };

    const cancelMapping = async () => {
        try {
            setCancellingMapping(true);
            const response = await axiosInstance.post('/onboarding/cancel-mapping', {
                userId: user.user_id
            });
            
            if (response.status === 200) {
                // Stop polling
                if (mappingCheckIntervalRef.current) {
                    clearInterval(mappingCheckIntervalRef.current);
                    mappingCheckIntervalRef.current = null;
                }
                isCheckingMappingRef.current = false;
                
                // Update UI
                setMappingLoading(false);
                setCancellingMapping(false);
            }
        } catch (e) {
            setCancellingMapping(false);
            console.error('Error cancelling mapping:', e);
            const errorMessage = e.response?.data?.detail || e.message || 'Failed to cancel mapping';
            setErrors((prev) => ({ ...prev, form: errorMessage }));
        }
    };

    const isFormValid = uploadedFiles.length > 0 || databaseUri.trim() !== '' || apiEndpoint.trim() !== '';

    return (
        <>
            {/* Mapping Loading Dialog */}
            <Dialog visible={mappingLoading} modal closable={false}
                style={{ width: '50vw' }} breakpoints={{ '960px': '75vw', '641px': '100vw' }}>
                <div className="flex items-center justify-center flex-col my-8">
                    <ProgressSpinner />
                    <Text className="text-xl text-black font-medium m-0 mt-4 mb-6 z-10 mx-4 sm:mx-auto">
                        We are mapping your data to our database. Please wait...
                    </Text>
                    <SecondaryButton 
                        label="Cancel"
                        danger
                        onClick={cancelMapping} 
                        disabled={cancellingMapping}
                        loading={cancellingMapping}
                        className="mt-4"
                    />
                </div>
            </Dialog>
            <div className="min-h-screen bg-gray-50 p-4 md:p-6 lg:p-8">
                <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start gap-4">
                    <Breadcrumb items={breadcrumbItems} />
                    <Text className="text-sm text-gray-500 m-0 font-medium w-24 mt-4">
                        Step 3 of 4
                    </Text>
                </div>

                <div className="max-w-6xl mx-auto">
                    <div className="bg-white rounded-2xl shadow-lg p-6 md:p-8 lg:p-10">
                        {
                            ingestionTypeLoading ? (
                                <div className="mb-6">
                                    <ProgressSpinner />
                                </div>
                            ) : ( 
                                <>                        
                                    <div className="mb-6">
                                        <Heading level={2} gradient={false} className="text-2xl md:text-3xl mb-2">
                                            Connect your data
                                        </Heading>
                                        <Text className="text-sm md:text-base text-gray-600 m-0">
                                            Follow the guidelines and submit to validate your data.
                                        </Text>
                                    </div>

                                    <div className="mb-8 bg-gray-50 rounded-lg p-6">
                                        <Heading level={3} black={true} className="text-lg font-bold mb-4" gradient={false}>
                                            Guidelines:
                                        </Heading>
                                        <ul className="space-y-3">
                                            <li className="flex items-start gap-2">
                                                <Text className="text-sm text-gray-700 m-0 flex-1">
                                                    • The data you provide will determine the scope of analytics, forecasts, and predictions generated by the system. You can upload any finite number of files, each of max 5 GB.
                                                </Text>
                                            </li>
                                            <li className="flex items-start gap-2">
                                                <Text className="text-sm text-gray-700 m-0 flex-1">
                                                    • A set of generalized data attributes is defined for the system.
                                                </Text>
                                            </li>
                                            <li className="flex items-start gap-2">
                                                <Text className="text-sm text-gray-700 m-0 flex-1">
                                                    • If your dataset contains all of these attributes, regardless of their exact names, the system will perform all analytics it is programmed for.
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
                                                    • Any analytics or forecasts requiring unmapped or missing attributes will be omitted.
                                                </Text>
                                            </li>
                                            <li className="flex items-start gap-2">
                                                <Text className="text-sm text-gray-700 m-0 flex-1">
                                                    • The guidelines on data preparation can be found from{' '}
                                                    <CustomLink href="#" gradient={true}>
                                                        here
                                                    </CustomLink>
                                                </Text>
                                            </li>
                                            <li className="flex items-start gap-2 mt-5">
                                                <Message
                                                    style={{
                                                        border: 'solid #00C597',
                                                        borderWidth: '0 0 0 6px',
                                                        color: '#00C597'
                                                    }}
                                                    className="w-full text-left flex justify-start items-start gap-3"
                                                    severity="success"
                                                    content={
                                                        <>
                                                            <div className="font-bold text-left my-4">
                                                                <div className="ml-2">NOTE: Please make sure you thoroughly read the guidelines before you proceed.</div>
                                                            </div>
                                                        </>
                                                    }
                                                />
                                            </li>
                                        </ul>
                                    </div>

                                    <form onSubmit={handleContinue} className="space-y-6">
                                        {
                                            ingestionType === 'batch' && (
                                                <>
                                                    <div className="space-y-4">
                                                        <input
                                                            ref={fileInputRef}
                                                            type="file"
                                                            multiple
                                                            accept=".csv,.xlsx,.xls,.parquet,.json"
                                                            onChange={handleFileInputChange}
                                                            className="hidden"
                                                        />
                                                        
                                                        <div
                                                            onClick={handleClickUploadArea}
                                                            onDragOver={handleDragOver}
                                                            onDragLeave={handleDragLeave}
                                                            onDrop={handleDrop}
                                                            className={`border-2 border-dashed rounded-xl p-8 md:p-12 text-center transition-colors cursor-pointer ${
                                                                isDragging 
                                                                    ? 'border-[var(--color-g2)] bg-blue-50' 
                                                                    : 'border-gray-300 hover:border-[var(--color-g2)]'
                                                            }`}
                                                        >
                                                            <div className="flex flex-col items-center gap-3">
                                                                <i className="pi pi-cloud-upload text-4xl text-gray-400"></i>
                                                                <Text className="text-gray-500 m-0">
                                                                    Click to Browse or Drag and Drop Files Here
                                                                </Text>
                                                                <Text className="text-xs text-gray-400 m-0">
                                                                    Supported formats: CSV, XLSX, XLS, Parquet, JSON <br/>
                                                                    Max file size: 5 GB
                                                                </Text>
                                                            </div>
                                                        </div>
                                                        
                                                        {uploadedFiles.length > 0 && (
                                                            <div className="flex flex-wrap gap-6 py-4">
                                                                {uploadedFiles.map((file) => {
                                                                    const progress = uploadProgress[file.fileId];
                                                                    const isUploading = progress !== undefined && progress < 100;

                                                                    return (
                                                                        <div
                                                                            key={file.fileId}
                                                                            className="flex flex-col items-center gap-2 relative group w-32"
                                                                        >
                                                                            {!isUploading && (
                                                                                <button
                                                                                    type="button"
                                                                                    onClick={() => handleFileRemove(file)}
                                                                                    className="absolute -top-2 -right-2 bg-red-500 text-white rounded-full w-5 h-5 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity z-10"
                                                                                    title="Remove file"
                                                                                >
                                                                                    <i className="pi pi-times text-xs"></i>
                                                                                </button>
                                                                            )}
                                                                            <div className="relative">
                                                                                <i className={`pi pi-file text-3xl ${isUploading ? 'text-gray-400' : 'text-[var(--color-g2)]'}`}></i>
                                                                                {isUploading && (
                                                                                    <div className="absolute inset-0 flex items-center justify-center">
                                                                                        <i className="pi pi-spin pi-spinner text-[var(--color-g2)]"></i>
                                                                                    </div>
                                                                                )}
                                                                            </div>
                                                                            <Text className="text-sm text-gray-700 m-0 text-center max-w-[120px] truncate">
                                                                                {file.name}
                                                                            </Text>
                                                                            <Text className="text-xs text-gray-500 m-0">
                                                                                {(file.size / 1024).toFixed(2)} KB
                                                                            </Text>
                                                                            
                                                                            {isUploading && (
                                                                                <div className="w-full">
                                                                                    <ProgressBar 
                                                                                        value={progress} 
                                                                                        style={{ height: '6px' }}
                                                                                        showValue={false}
                                                                                    />
                                                                                    <Text className="text-xs text-[var(--color-g2)] m-0 text-center mt-1">
                                                                                        {progress}%
                                                                                    </Text>
                                                                                </div>
                                                                            )}
                                                                            
                                                                            {progress === 100 && (
                                                                                <div className="absolute top-0 right-0 bg-green-500 text-white rounded-full w-5 h-5 flex items-center justify-center">
                                                                                    <i className="pi pi-check text-xs"></i>
                                                                                </div>
                                                                            )}
                                                                        </div>
                                                                    );
                                                                })}
                                                            </div>
                                                        )}
                                                    </div>
                                                </>
                                            )
                                        }
                                        {
                                            (ingestionType === 'db' || ingestionType === 'api') && (
                                                <>
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
        </>
    );
};

export default Connect;