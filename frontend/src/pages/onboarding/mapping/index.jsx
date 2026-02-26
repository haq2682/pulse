import React, { useState, useEffect, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router';
import { AutoComplete } from 'primereact/autocomplete';
import { Dialog } from 'primereact/dialog';
import { ProgressSpinner } from 'primereact/progressspinner';
import Breadcrumb from '../Breadcrumb';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import { SecondaryButton } from '@/components/global/Button';
import axiosInstance from '@/services/api/axiosInstance';
import { useAuth } from '@/context/AuthContext';
import usePageTitle from '@/hooks/usePageTitle';

// Utility functions for formatting names
const formatColumnName = (columnName) => {
    if (!columnName) return '';
    return columnName
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
};

const formatTableName = (tableName) => {
    if (!tableName) return '';
    return tableName
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
};

const formatColumnWithTable = (column, table) => {
    const formattedColumn = formatColumnName(column);
    const formattedTable = formatTableName(table);
    return `${formattedColumn} (${formattedTable} Table)`;
};

const Mapping = () => {
    usePageTitle('Onboarding - Map Data');
    const navigate = useNavigate();
    const location = useLocation();
    const pathname = location.pathname;
    const { user } = useAuth();
    
    const [dataLoading, setDataLoading] = useState(true);
    const [error, setError] = useState('');
    const [mappingInProgress, setMappingInProgress] = useState(false);  // Track if mapping is still running
    const [successMessage, setSuccessMessage] = useState(''); // Store success/warning message from API
    
    // Dialog state
    const [showConfirmDialog, setShowConfirmDialog] = useState(false);
    const [skippedColumns, setSkippedColumns] = useState([]);
    
    // Mapping loader state (for manual mapping processing)
    const [mappingLoading, setMappingLoading] = useState(false);
    const [cancellingMapping, setCancellingMapping] = useState(false);
    const mappingCheckIntervalRef = useRef(null);
    const isCheckingMappingRef = useRef(false);
    
    // Mapping data
    const [mappings, setMappings] = useState({});
    const [missingCols, setMissingCols] = useState([]);
    const [extraCols, setExtraCols] = useState([]);
    const [allFieldsIdentified, setAllFieldsIdentified] = useState(false);
    
    // AutoComplete state
    const [filteredColumns, setFilteredColumns] = useState({});

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
            active: false,
            clickable: false,
        },
        {
            label: 'Map',
            active: true,
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
            } else if (currentStep === 'data-type') {
                navigate(`/onboarding/data-type/${onboardingId}`);
            } else if (currentStep === 'connect' || currentStep === 'mapping-in-progress') {
                // Redirect to connect page if mapping is still in progress
                navigate(`/onboarding/connect/${onboardingId}`);
            } else if (currentStep === 'mapping') {
                return;
            } else {
                navigate(`/onboarding/connect/${onboardingId}`);
            }
        } catch (e) {
            console.error('Error fetching current step:', e);
            setError(e.response?.data?.detail || e.message || 'Failed to fetch current step');
        }
    };

    const fetchMappingData = async () => {
        setDataLoading(true);
        setError('');
        setSuccessMessage('');
        
        try {
            // First check mapping status
            const statusResponse = await axiosInstance.get('/onboarding/mapping-status', {
                params: { userId: user.user_id }
            });
            
            const { mapping_status } = statusResponse.data;
            
            // If mapping is still running, show spinner and don't fetch results yet
            if (mapping_status === 'running') {
                setMappingInProgress(true);
                setDataLoading(false);
                // Poll status every 3 seconds
                setTimeout(() => {
                    if (user?.user_id) {
                        fetchMappingData();
                    }
                }, 3000);
                return;
            }
            
            setMappingInProgress(false);
            
            // Fetch mapping results if completed or failed
            const response = await axiosInstance.get('/onboarding/mapping-results', {
                params: { userId: user.user_id }
            });
            
            if (response.status === 200) {
                const { missing_cols, extra_cols, all_fields_identified, message } = response.data;
                
                setMissingCols(missing_cols || []);
                setExtraCols(extra_cols || []);
                setAllFieldsIdentified(all_fields_identified || false);
                setSuccessMessage(message || ''); // Store the message from API
                
                // Initialize mappings object with null values for each missing column
                // Use :: as separator to avoid conflicts with table/column names containing underscores
                const initialMappings = {};
                (missing_cols || []).forEach((item) => {
                    const key = `${item.column}::${item.table}`;
                    initialMappings[key] = null;
                });
                setMappings(initialMappings);
            }
        } catch (e) {
            console.error('Error fetching mapping data:', e);
            setError(e.response?.data?.detail || e.message || 'Failed to fetch mapping data');
            setMappingInProgress(false);
        } finally {
            setDataLoading(false);
        }
    };

    useEffect(() => {
        const initializeData = async () => {
            await fetchCurrentStep();
            await fetchMappingData();
        };
        
        if (user?.user_id) {
            initializeData();
        }
        
        // Cleanup: clear interval on unmount
        return () => {
            if (mappingCheckIntervalRef.current) {
                clearInterval(mappingCheckIntervalRef.current);
                mappingCheckIntervalRef.current = null;
            }
        };
    }, [user?.user_id]);

    const handleMappingChange = (fieldKey, value) => {
        setMappings(prev => ({
            ...prev,
            [fieldKey]: value
        }));
    };

    const searchColumns = (event, fieldKey) => {
        const query = event.query.toLowerCase();
        
        // Filter extra columns based on search query
        const filtered = extraCols
            .filter(item => {
                const displayText = formatColumnWithTable(item.column, item.table).toLowerCase();
                return displayText.includes(query);
            })
            .map(item => ({
                label: formatColumnWithTable(item.column, item.table),
                value: item.column,
                table: item.table,
                originalColumn: item.column
            }));
        
        setFilteredColumns(prev => ({
            ...prev,
            [fieldKey]: filtered
        }));
    };

    const handleContinue = async (e) => {
        e.preventDefault();
        
        // Check if all required fields are mapped
        const unmappedFields = Object.keys(mappings).filter(key => !mappings[key]);
        
        if (unmappedFields.length > 0) {
            // Show confirmation dialog
            const skipped = unmappedFields.map(key => {
                const [column, table] = key.split('::');
                return formatColumnWithTable(column, table);
            });
            setSkippedColumns(skipped);
            setShowConfirmDialog(true);
        } else {
            // All fields mapped, proceed
            await saveMappings();
        }
    };

    const saveMappings = async () => {
        setMappingLoading(true);
        setError('');
        
        try {
            // Transform mappings to the format expected by backend
            const manualMappings = {};
            Object.keys(mappings).forEach(key => {
                if (mappings[key]) {
                    const [column, table] = key.split('::');
                    
                    if (!manualMappings[table]) {
                        manualMappings[table] = {};
                    }
                    
                    // Extract the actual column name from the selected value
                    const selectedValue = mappings[key];
                    const actualColumn = typeof selectedValue === 'object' ? selectedValue.originalColumn : selectedValue;
                    
                    manualMappings[table][column] = actualColumn;
                }
            });
            
            // Check if there are manual mappings
            const hasManualMappings = Object.keys(manualMappings).length > 0;
            
            if (hasManualMappings) {
                // Apply manual mappings to the already-mapped files in mapped-temp folder
                // This does NOT re-run the entire mapping pipeline
                const applyResponse = await axiosInstance.post('/onboarding/apply-manual-mappings', {
                    userId: user.user_id,
                    manualMappings: manualMappings
                });
                
                if (applyResponse.status === 200) {
                    
                    // Confirm mapping and navigate to dashboard
                    const response = await axiosInstance.post('/onboarding/confirm-mapping', {
                        userId: user.user_id
                    });
                    setMappingLoading(false);
                    navigate(`/analytics/${response.data.business_id}`);
                }
            } else {
                // No manual mappings, just confirm and proceed
                // This handles the case where all fields were already identified
                const response = await axiosInstance.post('/onboarding/confirm-mapping', {
                    userId: user.user_id
                });
                setMappingLoading(false);
                navigate(`/analytics/${response.data.business_id}`);
            }
        } catch (e) {
            console.error('Error saving mappings:', e);
            setError(e.response?.data?.detail || e.message || 'Failed to process mappings');
            setMappingLoading(false);
        }
    };

    const startMappingStatusCheck = () => {
        // Prevent duplicate interval creation
        if (isCheckingMappingRef.current) {
            return;
        }
        
        isCheckingMappingRef.current = true;
        let pollAttempts = 0;
        const MAX_POLL_ATTEMPTS = 200; // Maximum 200 attempts (200 * 5s = ~16 minutes)
        const POLL_INTERVAL = 5000; // Poll every 5 seconds
        
        // Poll every 5 seconds
        mappingCheckIntervalRef.current = setInterval(async () => {
            pollAttempts++;
            
            // Check if we've exceeded max attempts
            if (pollAttempts > MAX_POLL_ATTEMPTS) {
                clearInterval(mappingCheckIntervalRef.current);
                mappingCheckIntervalRef.current = null;
                isCheckingMappingRef.current = false;
                setMappingLoading(false);
                setError('Mapping is taking longer than expected. Please refresh the page to check status.');
                return;
            }
            
            try {
                const statusResponse = await axiosInstance.get('/onboarding/mapping-status', {
                    params: { userId: user.user_id }
                });
                
                const { mapping_status } = statusResponse.data;
                
                if (mapping_status === 'completed') {
                    // Stop polling
                    clearInterval(mappingCheckIntervalRef.current);
                    mappingCheckIntervalRef.current = null;
                    isCheckingMappingRef.current = false;
                    
                    // Confirm mapping and navigate to dashboard
                    try {
                        const response = await axiosInstance.post('/onboarding/confirm-mapping', {
                            userId: user.user_id
                        });
                        setMappingLoading(false);
                        navigate(`/analytics/${response.data.business_id}`);
                    } catch (confirmError) {
                        console.error('Error confirming mapping:', confirmError);
                        setError('Mapping completed but failed to confirm. Please try again.');
                        setMappingLoading(false);
                    }
                } else if (mapping_status === 'failed' || mapping_status === 'cancelled') {
                    // Stop polling
                    clearInterval(mappingCheckIntervalRef.current);
                    mappingCheckIntervalRef.current = null;
                    isCheckingMappingRef.current = false;
                    
                    setMappingLoading(false);
                    
                    if (mapping_status === 'failed') {
                        setError('Mapping pipeline failed. Please check your data and try again.');
                    } else {
                        setError('Mapping was cancelled.');
                    }
                }
                // If still running, continue polling
            } catch (statusError) {
                console.error('Error checking mapping status:', statusError);
                // Don't stop polling on transient errors, just log them
                // If there are persistent errors, the MAX_POLL_ATTEMPTS will catch it
            }
        }, POLL_INTERVAL);
    };

    const cancelMapping = async () => {
        try {
            setCancellingMapping(true);
            const response = await axiosInstance.post('/onboarding/cancel-mapping', {
                userId: user.user_id,
                duringManualMapping: true  // Indicate we're cancelling during manual mapping
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
                
                // Show message but don't navigate away - user stays on mapping page
                setError('Manual mapping was cancelled. You can adjust your mappings and try again.');
            }
        } catch (e) {
            setCancellingMapping(false);
            console.error('Error cancelling mapping:', e);
            const errorMessage = e.response?.data?.detail || e.message || 'Failed to cancel mapping';
            setError(errorMessage);
        }
    };

    const handleConfirmSkip = async () => {
        setShowConfirmDialog(false);
        await saveMappings();
    };

    const handleCancelSkip = () => {
        setShowConfirmDialog(false);
    };

    // Loading state
    if (dataLoading) {
        return (
            <div className="min-h-screen bg-gray-50 flex items-center justify-center">
                <div className="text-center">
                    <ProgressSpinner />
                    <Text className="text-gray-600 mt-4">Loading mapping data...</Text>
                </div>
            </div>
        );
    }
    
    // Mapping in progress state
    if (mappingInProgress) {
        return (
            <div className="min-h-screen bg-gray-50 flex items-center justify-center">
                <div className="text-center max-w-md mx-auto p-8">
                    <ProgressSpinner />
                    <Heading level={3} className="text-gray-800 mt-6 mb-4">Mapping Pipeline Running</Heading>
                    <Text className="text-gray-600">
                        The system is currently processing and mapping your data to the canonical schema.
                        This may take a few minutes depending on the size of your dataset.
                    </Text>
                    <Text className="text-gray-500 mt-4 text-sm">
                        This page will automatically update when mapping is complete.
                    </Text>
                </div>
            </div>
        );
    }

    return (
        <>
            {/* Confirmation Dialog */}
            <Dialog
                visible={showConfirmDialog}
                onHide={handleCancelSkip}
                header="Confirm Skipped Mappings"
                modal
                style={{ width: '50vw' }}
                breakpoints={{ '960px': '75vw', '641px': '100vw' }}
            >
                <div className="py-4">
                    <Text className="text-gray-700 mb-4">
                        You have not mapped the following columns:
                    </Text>
                    <ul className="list-disc pl-6 mb-4 text-gray-600">
                        {skippedColumns.map((col, idx) => (
                            <li key={idx} className="mb-1">{col}</li>
                        ))}
                    </ul>
                    <Text className="text-gray-700 font-semibold mb-2">
                        If you continue without mapping these columns:
                    </Text>
                    <Text className="text-gray-600 mb-4">
                        • Insights and analytics requiring these columns will not be generated<br />
                        • You may have limited functionality in your dashboard<br />
                    </Text>
                    <Text className="text-gray-700 font-semibold">
                        Do you want to continue without mapping these columns?
                    </Text>
                </div>
                <div className="flex justify-end gap-3 mt-4">
                    <button
                        onClick={handleCancelSkip}
                        className="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50"
                    >
                        Go Back
                    </button>
                    <PrimaryButton
                        label="Continue Anyway"
                        onClick={handleConfirmSkip}
                        loading={mappingLoading}
                    />
                </div>
            </Dialog>

            {/* Mapping Loading Dialog */}
            <Dialog 
                visible={mappingLoading} 
                modal 
                closable={false}
                style={{ width: '50vw' }} 
                breakpoints={{ '960px': '75vw', '641px': '100vw' }}
            >
                <div className="flex items-center justify-center flex-col my-8">
                    <ProgressSpinner />
                    <Text className="text-xl text-black font-medium m-0 mt-4 mb-6 z-10">
                        Applying your manual mappings to the data files. Please wait...
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
                {/* Breadcrumb and Step Indicator */}
                <div className="max-w-6xl mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start gap-4">
                    <Breadcrumb items={breadcrumbItems} />
                    <Text className="text-sm text-gray-500 m-0 font-medium w-24 mt-4">
                        Step 4 of 4
                    </Text>
                </div>

                {/* Main Card */}
                <div className="max-w-6xl mx-auto">
                    <div className="bg-white rounded-2xl shadow-lg p-6 md:p-8 lg:p-10">
                        {/* Header */}
                        <div className="mb-6">
                            <Heading level={2} gradient={false} className="text-2xl md:text-3xl mb-2">
                                Map Your Data
                            </Heading>
                            <Text className="text-sm md:text-base text-gray-600 m-0">
                                Map the data fields that the system was unable to identify automatically
                            </Text>
                        </div>

                        {/* Error Message */}
                        {error && (
                            <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
                                <Text className="text-red-600 m-0">
                                    {error}
                                </Text>
                            </div>
                        )}

                        {/* Success/Warning Message from API */}
                        {successMessage && (
                            <div className={`mb-6 p-4 rounded-lg border ${
                                allFieldsIdentified 
                                    ? 'bg-green-50 border-green-200' 
                                    : 'bg-yellow-50 border-yellow-200'
                            }`}>
                                <Text className={`font-medium m-0 ${
                                    allFieldsIdentified 
                                        ? 'text-green-700' 
                                        : 'text-yellow-700'
                                }`}>
                                    {successMessage}
                                </Text>
                            </div>
                        )}

                        {/* Success Message - Show if all fields identified */}
                        {allFieldsIdentified && missingCols.length === 0 && (
                            <div className="mb-8 py-6 border-y border-gray-200">
                                <Heading level={3} gradient={true} className="text-lg md:text-xl text-center m-0">
                                    The system identified all the required data fields. You do not need to map anything manually.
                                </Heading>
                            </div>
                        )}

                        {/* Fields to Map Message - Show if some fields need mapping */}
                        {missingCols.length > 0 && (
                            <div className="mb-6 py-4 border-y border-gray-200">
                                <Text className="text-center text-gray-700 m-0">
                                    The following fields could not be automatically identified. Please map them to your data columns:
                                </Text>
                            </div>
                        )}

                        {/* Note */}
                        {missingCols.length > 0 && (
                            <div className="mb-8 bg-green-50 border-l-4 border-[var(--color-g2)] p-4 rounded">
                                <Text className="text-sm text-gray-700 m-0">
                                    <span className="font-bold text-[var(--color-g1)]">Note:</span> If you skip providing any required data field from the list, then the system will automatically omit the analysis which requires that particular field(s).
                                </Text>
                            </div>
                        )}

                        {/* Form */}
                        <form onSubmit={handleContinue} className="space-y-6">
                            {/* Dynamic Mapping Fields Grid */}
                            {missingCols.length > 0 && (
                                <div className="grid grid-cols-1 gap-y-6">
                                    {missingCols.map((item, idx) => {
                                        const fieldKey = `${item.column}::${item.table}`;
                                        const displayLabel = formatColumnWithTable(item.column, item.table);
                                        
                                        return (
                                            <div key={idx} className="flex flex-col sm:flex-row items-start gap-3">
                                                <label
                                                    htmlFor={fieldKey}
                                                    className="text-sm font-semibold text-gray-900 sm:min-w-[220px] sm:text-right sm:pt-2"
                                                >
                                                    {displayLabel}:
                                                </label>
                                                <div className="flex-1 w-full">
                                                    <AutoComplete
                                                        id={fieldKey}
                                                        value={mappings[fieldKey]}
                                                        suggestions={filteredColumns[fieldKey] || []}
                                                        completeMethod={(e) => searchColumns(e, fieldKey)}
                                                        onChange={(e) => handleMappingChange(fieldKey, e.value)}
                                                        field="label"
                                                        placeholder="Search and select a column"
                                                        className="w-full"
                                                        inputClassName="w-full"
                                                        disabled={mappingLoading}
                                                        dropdown
                                                        forceSelection={false}
                                                    />
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            )}

                            {/* No Fields to Map Message */}
                            {missingCols.length === 0 && allFieldsIdentified && (
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
                                    type="submit"
                                    loading={mappingLoading}
                                    disabled={dataLoading || mappingLoading}
                                    className="px-8"
                                />
                            </div>
                        </form>
                    </div>
                </div>
            </div>
        </>
    );
};

export default Mapping;
