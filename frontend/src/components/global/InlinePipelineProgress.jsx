import React, { useEffect } from 'react';
import { Knob } from 'primereact/knob';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import { PrimaryButton } from '@/components/global/Button';
import Text from '@/components/global/Typography/Text';
import Heading from '@/components/global/Typography/Heading';

const InlinePipelineProgress = ({ businessId, onStartAnalysis }) => {
    const {
        pipelineStatus,
        isConnected,
        connectWebSocket,
        disconnectWebSocket,
        fetchPipelineStatus,
        cancelPipeline,
        retryPipeline,
        startPipeline
    } = usePipelineProgress();
    
    const [isCancelling, setIsCancelling] = React.useState(false);
    const [isRetrying, setIsRetrying] = React.useState(false);
    const [isStarting, setIsStarting] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState(null);
    const previousBusinessIdRef = React.useRef(null);
    
    // Connect WebSocket when business changes
    useEffect(() => {
        if (businessId && businessId !== previousBusinessIdRef.current) {
            // Fetch initial status
            fetchPipelineStatus(businessId);
            
            // Connect WebSocket
            connectWebSocket(businessId);
            
            previousBusinessIdRef.current = businessId;
            
            return () => {
                // Only disconnect when component unmounts or business changes
                disconnectWebSocket();
                previousBusinessIdRef.current = null;
            };
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [businessId]); // Only depend on businessId to prevent reconnection loops
    
    // Get progress percentage
    const progress = pipelineStatus?.progress || 0;
    
    // Determine pipeline state
    const businessLoading = !pipelineStatus && !errorMessage || pipelineStatus === 'loading';
    const isRunning = pipelineStatus?.status === 'running';
    // const isCompleted = pipelineStatus?.status === 'completed';
    const isFailed = pipelineStatus?.status === 'failed';
    const hasNoPipeline = !pipelineStatus || pipelineStatus.status === 'cancelled';
    
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
    
    // Handle start analysis
    const handleStartAnalysis = async () => {
        setIsStarting(true);
        setErrorMessage(null);
        
        if (onStartAnalysis) {
            await onStartAnalysis();
        }
        
        setIsStarting(false);
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
            
            if (!result.success) {
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
        } catch (err) {
            console.error('Error retrying pipeline:', err);
            setErrorMessage('Error retrying pipeline');
        } finally {
            setIsRetrying(false);
        }
    };
    
    if(businessLoading) {
        return (
            <div className="flex flex-col items-center justify-center min-h-[60vh] p-8">
                <div className="text-center max-w-2xl">
                    <div className="mb-6 place-self-center">
                        <i className="pi pi-spin pi-spinner text-6xl text-gray-300 mb-4"></i>
                    </div>
                    <Heading level={3} className="mb-3">
                        Loading Business Data...
                    </Heading>
                    <Text className="text-gray-600 mb-6">
                        Please wait while we load your business data and pipeline status.
                    </Text>
                </div>
            </div>
        );
    }

    // Show "Start Analysis" button if no pipeline exists
    if (hasNoPipeline) {
        return (
            <div className="flex flex-col items-center justify-center min-h-[60vh] p-8">
                <div className="text-center max-w-2xl">
                    <div className="mb-6 place-self-center">
                        <i className="pi pi-chart-line text-6xl text-gray-300 mb-4"></i>
                    </div>
                    <Heading level={3} className="mb-3">
                        Ready to Analyze Your Data
                    </Heading>
                    <Text className="text-gray-600 mb-6">
                        Start the analysis pipeline to process your data through cleaning, 
                        transformation, analysis, and machine learning phases.
                    </Text>
                    <PrimaryButton
                        label="Start Analysis"
                        onClick={handleStartAnalysis}
                        loading={isStarting}
                        className="px-8 py-3"
                    />
                    {errorMessage && (
                        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                            <Text className="text-red-600 text-sm m-0">
                                {errorMessage}
                            </Text>
                        </div>
                    )}
                </div>
            </div>
        );
    }
    
    // Show pipeline in progress
    if (isRunning) {
        return (
            <div className="flex flex-col items-center justify-center min-h-[60vh] p-8">
                <div className="text-center max-w-2xl">
                    {/* Knob */}
                    <div className="mb-6 place-self-center">
                        <Knob
                            value={progress}
                            readOnly
                            size={180}
                            valueColor={getStatusColor()}
                            rangeColor="#e5e7eb"
                            textColor={getStatusColor()}
                            strokeWidth={10}
                            valueTemplate={'{value}%'}
                        />
                    </div>
                    
                    {/* Status Message */}
                    <Heading level={3} className="mb-2">
                        Processing Your Data
                    </Heading>
                    <Text className="text-gray-600 mb-1">
                        {pipelineStatus.current_step || 'Pipeline running...'}
                    </Text>
                    <Text className="text-sm text-gray-500 mb-6">
                        {isConnected ? '● Connected' : '○ Connecting...'}
                    </Text>
                    
                    {/* Pipeline Phases */}
                    <div className="mb-6 w-full max-w-md mx-auto">
                        <Text className="text-sm font-semibold text-gray-700 mb-2">
                            Pipeline Phases:
                        </Text>
                        <div className="space-y-1 text-sm text-gray-600 text-left">
                            <div className={progress >= 0 ? 'text-green-600 font-medium' : ''}>
                                {progress > 25 ? '✓' : '○'} Cleaning Data (0-25%)
                            </div>
                            <div className={progress >= 25 ? 'text-green-600 font-medium' : ''}>
                                {progress > 55 ? '✓' : progress >= 25 ? '○' : '○'} Transforming & Aggregating (25-55%)
                            </div>
                            <div className={progress >= 55 ? 'text-green-600 font-medium' : ''}>
                                {progress > 85 ? '✓' : progress >= 55 ? '○' : '○'} Analyzing Data (55-85%)
                            </div>
                            <div className={progress >= 85 ? 'text-green-600 font-medium' : ''}>
                                {progress >= 100 ? '✓' : progress >= 85 ? '○' : '○'} Running ML Predictions (85-100%)
                            </div>
                        </div>
                    </div>
                    
                    {/* Cancel Button */}
                    <PrimaryButton
                        label="Cancel Pipeline"
                        onClick={handleCancel}
                        loading={isCancelling}
                        danger
                        className="text-sm"
                    />
                    
                    {errorMessage && (
                        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                            <Text className="text-red-600 text-sm m-0">
                                {errorMessage}
                            </Text>
                        </div>
                    )}
                </div>
            </div>
        );
    }
    
    // Show completed status
    // if (isCompleted) {
    //     return (
    //         <div className="flex flex-col items-center justify-center min-h-[60vh] p-8">
    //             <div className="text-center max-w-2xl">
    //                 <div className="mb-6 place-self-center">
    //                     <Knob
    //                         value={100}
    //                         readOnly
    //                         size={180}
    //                         valueColor="#22c55e"
    //                         rangeColor="#e5e7eb"
    //                         textColor="#22c55e"
    //                         strokeWidth={10}
    //                         valueTemplate={'{value}%'}
    //                     />
    //                 </div>
                    
    //                 <div className="mb-4">
    //                     <i className="pi pi-check-circle text-5xl text-green-600"></i>
    //                 </div>
                    
    //                 <Heading level={3} className="text-green-600 mb-2">
    //                     Analysis Complete!
    //                 </Heading>
    //                 <Text className="text-gray-600">
    //                     Your data has been successfully processed and is ready for visualization.
    //                 </Text>
    //             </div>
    //         </div>
    //     );
    // }
    
    // Show failed status with retry button
    if (isFailed) {
        return (
            <div className="flex flex-col items-center justify-center min-h-[60vh] p-8">
                <div className="text-center max-w-2xl">
                    <div className="mb-6 place-self-center">
                        <Knob
                            value={progress}
                            readOnly
                            size={180}
                            valueColor="#ef4444"
                            rangeColor="#e5e7eb"
                            textColor="#ef4444"
                            strokeWidth={10}
                            valueTemplate={'{value}%'}
                        />
                    </div>
                    
                    <div className="mb-4">
                        <i className="pi pi-exclamation-triangle text-5xl text-red-600"></i>
                    </div>
                    
                    <Heading level={3} className="text-red-600 mb-2">
                        Analysis Failed
                    </Heading>
                    <Text className="text-gray-600 mb-2">
                        An error occurred while processing your data.
                    </Text>
                    
                    {pipelineStatus.error_message && (
                        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                            <Text className="text-red-600 text-sm m-0">
                                {pipelineStatus.error_message}
                            </Text>
                        </div>
                    )}
                    
                    {pipelineStatus.failed_phase && (
                        <Text className="text-gray-500 text-sm mb-4">
                            Failed at: <span className="font-semibold">{pipelineStatus.failed_phase}</span> phase
                        </Text>
                    )}
                    
                    <div className="mt-6">
                        <PrimaryButton
                            label={pipelineStatus.failed_phase ? `Retry from ${pipelineStatus.failed_phase}` : "Retry Analysis"}
                            onClick={handleRetry}
                            loading={isRetrying}
                            icon="pi pi-refresh"
                            className="px-8"
                        />
                    </div>
                    
                    {errorMessage && (
                        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                            <Text className="text-red-600 text-sm m-0">
                                {errorMessage}
                            </Text>
                        </div>
                    )}
                </div>
            </div>
        );
    }
    
    return null;
};

export default InlinePipelineProgress;
