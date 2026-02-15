import React, { useEffect } from 'react';
import { Dialog } from 'primereact/dialog';
import { Knob } from 'primereact/knob';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import { PrimaryButton, SecondaryButton } from '@/components/global/Button';
import Text from '@/components/global/Typography/Text';
import Heading from '@/components/global/Typography/Heading';

const PipelineProgressLoader = ({ businessId, visible = false, onComplete }) => {
    const {
        pipelineStatus,
        isConnected,
        connectWebSocket,
        disconnectWebSocket,
        fetchPipelineStatus,
        cancelPipeline,
        retryPipeline
    } = usePipelineProgress();
    
    const [showCancelConfirm, setShowCancelConfirm] = React.useState(false);
    const [isCancelling, setIsCancelling] = React.useState(false);
    const [isRetrying, setIsRetrying] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState(null);
    
    // Connect WebSocket when business changes
    useEffect(() => {
        if (businessId && visible) {
            // Fetch initial status
            fetchPipelineStatus(businessId);
            
            // Connect WebSocket
            connectWebSocket(businessId);
            
            return () => {
                disconnectWebSocket();
            };
        }
    }, [businessId, visible, connectWebSocket, disconnectWebSocket, fetchPipelineStatus]);
    
    // Determine if we should show the loader
    const shouldShowLoader = visible && pipelineStatus && 
        (pipelineStatus.status === 'running' || 
         pipelineStatus.status === 'completed' || 
         pipelineStatus.status === 'failed');
    
    // Get progress percentage
    const progress = pipelineStatus?.progress || 0;
    
    // Get status color
    const getStatusColor = () => {
        if (!pipelineStatus) return 'var(--color-g2)';
        
        switch (pipelineStatus.status) {
            case 'running':
                return 'var(--color-g2)';
            case 'completed':
                return '#22c55e'; // green
            case 'failed':
                return '#ef4444'; // red
            default:
                return 'var(--color-g2)';
        }
    };
    
    // Handle cancel
    const handleCancel = async () => {
        if (!pipelineStatus?.pipeline_id) return;
        
        setIsCancelling(true);
        setErrorMessage(null);
        try {
            const result = await cancelPipeline(
                pipelineStatus.pipeline_id,
                businessId,
                true // cleanup data
            );
            
            if (result.success) {
                setShowCancelConfirm(false);
                // Status will be updated via WebSocket
            } else {
                console.error('Failed to cancel pipeline:', result.error);
                setErrorMessage('Failed to cancel pipeline: ' + result.error);
            }
        } catch (err) {
            console.error('Error cancelling pipeline:', err);
            setErrorMessage('Error cancelling pipeline');
        } finally {
            setIsCancelling(false);
        }
    };
    
    // Handle retry
    const handleRetry = async () => {
        setIsRetrying(true);
        setErrorMessage(null);
        try {
            const result = await retryPipeline(businessId);
            
            if (!result.success) {
                console.error('Failed to retry pipeline:', result.error);
                setErrorMessage('Failed to retry pipeline: ' + result.error);
            }
            // Status will be updated via WebSocket
        } catch (err) {
            console.error('Error retrying pipeline:', err);
            setErrorMessage('Error retrying pipeline');
        } finally {
            setIsRetrying(false);
        }
    };
    
    // Close/dismiss handler
    const handleClose = () => {
        if (onComplete) {
            onComplete();
        }
        // Reset error state
        setErrorMessage(null);
    };
    
    if (!shouldShowLoader) {
        return null;
    }
    
    const isRunning = pipelineStatus.status === 'running';
    const isCompleted = pipelineStatus.status === 'completed';
    const isFailed = pipelineStatus.status === 'failed';
    
    return (
        <>
            {/* Main Pipeline Progress Dialog */}
            <Dialog
                visible={shouldShowLoader}
                modal
                closable={false}
                draggable={false}
                resizable={false}
                style={{ width: '500px' }}
                breakpoints={{ '960px': '75vw', '641px': '90vw' }}
            >
                <div className="flex flex-col items-center justify-center py-6">
                    {/* Knob */}
                    <Knob
                        value={progress}
                        readOnly
                        size={200}
                        valueColor={getStatusColor()}
                        rangeColor="#e5e7eb"
                        textColor={getStatusColor()}
                        strokeWidth={10}
                        valueTemplate={'{value}%'}
                    />
                    
                    {/* Status Message */}
                    <div className="mt-6 text-center">
                        {isRunning && (
                            <>
                                <Heading level={3} className="mb-2">
                                    Processing Your Data
                                </Heading>
                                <Text className="text-gray-600 mb-1">
                                    {pipelineStatus.current_step || 'Pipeline running...'}
                                </Text>
                                <Text className="text-sm text-gray-500">
                                    {isConnected ? '● Connected' : '○ Connecting...'}
                                </Text>
                            </>
                        )}
                        
                        {isCompleted && (
                            <>
                                <Heading level={3} className="text-green-600 mb-2">
                                    Pipeline Completed Successfully!
                                </Heading>
                                <Text className="text-gray-600 mb-4">
                                    Your data has been processed and is ready for analysis.
                                </Text>
                                <PrimaryButton
                                    label="Continue to Dashboard"
                                    onClick={handleClose}
                                    className="mt-2"
                                />
                            </>
                        )}
                        
                        {isFailed && (
                            <>
                                <Heading level={3} className="text-red-600 mb-2">
                                    Processing Failed
                                </Heading>
                                <Text className="text-gray-600 mb-2">
                                    An error occurred while trying to process your data.
                                </Text>
                                {pipelineStatus.error_message && (
                                    <Text className="text-sm text-gray-500 mb-4">
                                        {pipelineStatus.error_message}
                                    </Text>
                                )}
                                {errorMessage && (
                                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                                        <Text className="text-red-600 text-sm m-0">
                                            {errorMessage}
                                        </Text>
                                    </div>
                                )}
                                <div className="flex gap-3 justify-center mt-4">
                                    <SecondaryButton
                                        label="Retry"
                                        onClick={handleRetry}
                                        loading={isRetrying}
                                        icon="pi pi-refresh"
                                    />
                                    <PrimaryButton
                                        label="Close"
                                        onClick={handleClose}
                                    />
                                </div>
                            </>
                        )}
                    </div>
                    
                    {/* Cancel Button - Only show when running */}
                    {isRunning && (
                        <div className="mt-6">
                            <SecondaryButton
                                label="Cancel Pipeline"
                                onClick={() => setShowCancelConfirm(true)}
                                danger
                                icon="pi pi-times"
                                className="text-sm"
                            />
                        </div>
                    )}
                    
                    {/* Pipeline Phases */}
                    {isRunning && (
                        <div className="mt-6 w-full px-4">
                            <Text className="text-sm font-semibold text-gray-700 mb-2">
                                Pipeline Phases:
                            </Text>
                            <div className="space-y-1 text-sm text-gray-600">
                                <div className={progress >= 0 ? 'text-green-600' : ''}>
                                    ✓ Cleaning Data (0-25%)
                                </div>
                                <div className={progress >= 25 ? 'text-green-600' : ''}>
                                    {progress >= 25 ? '✓' : '○'} Transforming & Aggregating (25-55%)
                                </div>
                                <div className={progress >= 55 ? 'text-green-600' : ''}>
                                    {progress >= 55 ? '✓' : '○'} Analyzing Data (55-85%)
                                </div>
                                <div className={progress >= 85 ? 'text-green-600' : ''}>
                                    {progress >= 85 ? '✓' : '○'} Running ML Predictions (85-100%)
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </Dialog>
            
            {/* Cancel Confirmation Dialog */}
            <Dialog
                visible={showCancelConfirm}
                onHide={() => !isCancelling && setShowCancelConfirm(false)}
                header="Cancel Pipeline?"
                modal
                style={{ width: '450px' }}
                breakpoints={{ '960px': '75vw', '641px': '90vw' }}
            >
                <div className="py-4">
                    <Text className="text-gray-700 mb-4">
                        Are you sure you want to cancel the pipeline?
                    </Text>
                    <Text className="text-gray-600 text-sm mb-4">
                        This will:
                    </Text>
                    <ul className="list-disc pl-6 mb-4 text-gray-600 text-sm">
                        <li>Stop all running processes</li>
                        <li>Delete partially processed data from storage</li>
                        <li>Require you to restart the pipeline from the beginning</li>
                    </ul>
                    <Text className="text-red-600 text-sm font-semibold">
                        This action cannot be undone.
                    </Text>
                    {errorMessage && (
                        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                            <Text className="text-red-600 text-sm m-0">
                                {errorMessage}
                            </Text>
                        </div>
                    )}
                </div>
                <div className="flex justify-end gap-3 mt-4">
                    <SecondaryButton
                        label="Go Back"
                        onClick={() => setShowCancelConfirm(false)}
                        disabled={isCancelling}
                    />
                    <PrimaryButton
                        label="Yes, Cancel Pipeline"
                        onClick={handleCancel}
                        loading={isCancelling}
                        danger
                    />
                </div>
            </Dialog>
        </>
    );
};

export default PipelineProgressLoader;
