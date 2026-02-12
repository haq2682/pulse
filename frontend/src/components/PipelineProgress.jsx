import React from 'react';
import { Knob } from 'primereact/knob';
import { Dialog } from 'primereact/dialog';
import { usePipeline } from '@/context/PipelineContext';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import { SecondaryButton } from '@/components/global/Button';

const PipelineProgress = () => {
    const {
        pipelineStatus,
        currentPhase,
        progressPercentage,
        stepDescription,
        errorMessage,
        isLoading,
        cancelPipeline,
        retryPipeline
    } = usePipeline();

    const [showDialog, setShowDialog] = React.useState(false);
    const [cancelling, setCancelling] = React.useState(false);

    // Don't show anything if no pipeline exists or it's completed without errors
    if (!pipelineStatus || (pipelineStatus === 'completed' && !showDialog)) {
        return null;
    }

    // Show dialog for failed state
    React.useEffect(() => {
        if (pipelineStatus === 'failed') {
            setShowDialog(true);
        }
    }, [pipelineStatus]);

    const handleCancel = async () => {
        setCancelling(true);
        await cancelPipeline();
        setCancelling(false);
    };

    const handleRetry = async () => {
        setShowDialog(false);
        await retryPipeline();
    };

    const getPhaseLabel = (phase) => {
        const labels = {
            'cleaning': 'Data Cleaning',
            'transformation': 'Data Transformation',
            'analysis': 'Data Analysis',
            'machine-learning': 'ML Inference'
        };
        return labels[phase] || phase;
    };

    const getStatusColor = () => {
        switch (pipelineStatus) {
            case 'running':
                return 'var(--color-primary)';
            case 'completed':
                return '#22c55e'; // green
            case 'failed':
            case 'cancelled':
                return '#ef4444'; // red
            default:
                return 'var(--color-primary)';
        }
    };

    return (
        <>
            {/* Global Pipeline Progress Indicator */}
            {pipelineStatus === 'running' && (
                <div className="fixed top-20 right-6 z-50 bg-white rounded-lg shadow-xl p-4 border border-gray-200">
                    <div className="flex flex-col items-center gap-3">
                        <Knob
                            value={progressPercentage}
                            readOnly
                            size={120}
                            valueColor={getStatusColor()}
                            rangeColor="#e5e7eb"
                            textColor="#1f2937"
                        />
                        
                        <div className="text-center">
                            <p className="text-sm font-semibold text-gray-700">
                                Pipeline Processing
                            </p>
                            {currentPhase && (
                                <p className="text-xs text-gray-500 mt-1">
                                    {getPhaseLabel(currentPhase)}
                                </p>
                            )}
                            {stepDescription && (
                                <p className="text-xs text-gray-400 mt-1 max-w-[200px]">
                                    {stepDescription}
                                </p>
                            )}
                        </div>

                        <SecondaryButton
                            onClick={handleCancel}
                            disabled={cancelling || isLoading}
                            className="text-xs py-1 px-3"
                        >
                            {cancelling ? 'Cancelling...' : 'Cancel'}
                        </SecondaryButton>
                    </div>
                </div>
            )}

            {/* Completed State Indicator */}
            {pipelineStatus === 'completed' && showDialog && (
                <div className="fixed top-20 right-6 z-50 bg-white rounded-lg shadow-xl p-4 border border-green-200">
                    <div className="flex flex-col items-center gap-3">
                        <Knob
                            value={100}
                            readOnly
                            size={120}
                            valueColor="#22c55e"
                            rangeColor="#e5e7eb"
                            textColor="#1f2937"
                        />
                        
                        <div className="text-center">
                            <p className="text-sm font-semibold text-green-700">
                                Pipeline Completed!
                            </p>
                            <p className="text-xs text-gray-500 mt-1">
                                Pipeline has completed execution
                            </p>
                        </div>

                        <SecondaryButton
                            onClick={() => setShowDialog(false)}
                            className="text-xs py-1 px-3"
                        >
                            Dismiss
                        </SecondaryButton>
                    </div>
                </div>
            )}

            {/* Error Dialog */}
            <Dialog
                header="Pipeline Error"
                visible={pipelineStatus === 'failed' && showDialog}
                onHide={() => setShowDialog(false)}
                style={{ width: '450px' }}
                modal
            >
                <div className="flex flex-col items-center gap-4 py-4">
                    <i className="pi pi-times-circle text-red-500 text-6xl"></i>
                    
                    <div className="text-center">
                        <p className="text-lg font-semibold text-gray-800 mb-2">
                            An error occurred
                        </p>
                        <p className="text-sm text-gray-600">
                            An error occurred while trying to process your data. Please try again.
                        </p>
                        {errorMessage && (
                            <p className="text-xs text-gray-500 mt-2 bg-gray-50 p-2 rounded">
                                {errorMessage}
                            </p>
                        )}
                    </div>

                    <div className="flex gap-3 mt-4">
                        <SecondaryButton onClick={() => setShowDialog(false)}>
                            Close
                        </SecondaryButton>
                        <PrimaryButton 
                            onClick={handleRetry}
                            disabled={isLoading}
                        >
                            {isLoading ? 'Retrying...' : 'Retry Pipeline'}
                        </PrimaryButton>
                    </div>
                </div>
            </Dialog>
        </>
    );
};

export default PipelineProgress;
