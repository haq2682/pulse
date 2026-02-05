import React, { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router';
import { InputText } from 'primereact/inputtext';
import { ProgressBar } from 'primereact/progressbar';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import axiosInstance from '@/services/api/axiosInstance';
import { useAuth } from '@/context/AuthContext';

const CHUNK_SIZE = 5 * 1024 * 1024;

const Connect = () => {
    const navigate = useNavigate();
    const [databaseUri, setDatabaseUri] = useState('');
    const [apiEndpoint, setApiEndpoint] = useState('');
    const { user } = useAuth();
    const [uploadedFiles, setUploadedFiles] = useState([]);
    const [uploadProgress, setUploadProgress] = useState({});
    const [loading, setLoading] = useState(false);
    const [ingestionTypeLoading, setIngestionTypeLoading] = useState(true);
    const [ingestionType, setIngestionType] = useState('');
    const [errors, setErrors] = useState({db: '', api: '', form: ''});
    const [isDragging, setIsDragging] = useState(false);
    const fileInputRef = useRef(null);

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

    useEffect(() => {
        fetchIngestionType();
        fetchUploadedFiles();
    }, []);

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

    const uploadFileInChunks = async (file, fileId) => {
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

        try {
            for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
                const start = chunkIndex * CHUNK_SIZE;
                const end = Math.min(start + CHUNK_SIZE, file.size);
                const chunk = file.slice(start, end);

                const formData = new FormData();
                formData.append('chunk', chunk);
                formData.append('chunkIndex', chunkIndex);
                formData.append('totalChunks', totalChunks);
                formData.append('fileId', fileId);
                formData.append('fileName', file.name);
                formData.append('fileSize', file.size);
                formData.append('fileType', file.type);
                formData.append('userId', user.user_id);

                await axiosInstance.post('/onboarding/upload-chunk', formData, {
                    headers: { 'Content-Type': 'multipart/form-data' }
                });

                const progress = Math.round(((chunkIndex + 1) / totalChunks) * 100);
                setUploadProgress(prev => ({ ...prev, [fileId]: progress }));
            }

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

        } catch (error) {
            console.error('Upload error:', error);
            setUploadedFiles(prev => prev.filter(f => f.fileId !== fileId));
            setUploadProgress(prev => {
                const newProgress = { ...prev };
                delete newProgress[fileId];
                return newProgress;
            });
            throw error;
        }
    };

    const validateFile = (file) => {
        const allowedExtensions = ['csv', 'xlsx', 'xls', 'parquet', 'json'];
        const fileName = file.name.toLowerCase();
        const fileExtension = fileName.split('.').pop();
        return allowedExtensions.includes(fileExtension);
    };

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
            
            uploadFileInChunks(file, fileId).catch(err => {
                setErrors(prev => ({ ...prev, form: `Failed to upload ${file.name}` }));
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
        
        setLoading(false);
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

    const isFormValid = uploadedFiles.length > 0 || databaseUri.trim() !== '' || apiEndpoint.trim() !== '';

    return (
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
                                <Text className="text-base text-gray-600 m-0 text-center">
                                    Loading...
                                </Text>
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
                                                • The data you provide will determine the scope of analytics, forecasts, and predictions generated by the system. You can upload any finite number of files.
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
                                                • The generalized data attributes can be downloaded from{' '}
                                                <CustomLink href="#" gradient={true}>
                                                    here
                                                </CustomLink>
                                            </Text>
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
                                                                Supported formats: CSV, XLSX, XLS, Parquet, JSON
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
    );
};

export default Connect;