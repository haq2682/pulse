import React, { createContext, useContext, useState, useEffect, useRef } from 'react';
import axiosInstance from '@/services/api/axiosInstance';
import { useAuth } from './AuthContext';

const PipelineContext = createContext();

export const usePipeline = () => {
    const context = useContext(PipelineContext);
    if (!context) {
        throw new Error('usePipeline must be used within PipelineProvider');
    }
    return context;
};

export const PipelineProvider = ({ children }) => {
    const { user } = useAuth();
    const [pipelineStatus, setPipelineStatus] = useState(null);
    const [pipelineId, setPipelineId] = useState(null);
    const [currentPhase, setCurrentPhase] = useState(null);
    const [progressPercentage, setProgressPercentage] = useState(0);
    const [stepDescription, setStepDescription] = useState('');
    const [errorMessage, setErrorMessage] = useState(null);
    const [isLoading, setIsLoading] = useState(false);
    
    const eventSourceRef = useRef(null);
    const reconnectTimeoutRef = useRef(null);

    // Fetch initial pipeline status
    const fetchPipelineStatus = async () => {
        if (!user?.user_id) return;
        
        try {
            const response = await axiosInstance.get(`/pipeline/status?userId=${user.user_id}`);
            const data = response.data;
            
            if (data.pipeline_status) {
                setPipelineId(data.pipeline_id);
                setPipelineStatus(data.pipeline_status);
                setCurrentPhase(data.current_phase);
                setProgressPercentage(data.progress_percentage || 0);
                setStepDescription(data.step_description || '');
                setErrorMessage(data.error_message);
                
                // Start SSE connection if pipeline is running
                if (data.pipeline_status === 'running') {
                    connectSSE();
                }
            }
        } catch (error) {
            console.error('Error fetching pipeline status:', error);
        }
    };

    // Connect to Server-Sent Events for real-time updates
    const connectSSE = () => {
        if (!user?.user_id || eventSourceRef.current) return;

        try {
            const eventSource = new EventSource(
                `${axiosInstance.defaults.baseURL}/pipeline/status-stream?userId=${user.user_id}`
            );

            eventSource.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    if (data.type === 'status_update') {
                        setPipelineId(data.pipeline_id);
                        setPipelineStatus(data.status);
                        setCurrentPhase(data.current_phase);
                        setProgressPercentage(data.progress_percentage || 0);
                        setStepDescription(data.step_description || '');
                        setErrorMessage(data.error_message);
                        
                        // Close connection if pipeline finished
                        if (['completed', 'failed', 'cancelled'].includes(data.status)) {
                            disconnectSSE();
                        }
                    } else if (data.type === 'stream_end' || data.type === 'error') {
                        disconnectSSE();
                    }
                } catch (error) {
                    console.error('Error parsing SSE message:', error);
                }
            };

            eventSource.onerror = (error) => {
                console.error('SSE connection error:', error);
                disconnectSSE();
                
                // Retry connection after 5 seconds if pipeline is still running
                if (pipelineStatus === 'running') {
                    reconnectTimeoutRef.current = setTimeout(() => {
                        connectSSE();
                    }, 5000);
                }
            };

            eventSourceRef.current = eventSource;
        } catch (error) {
            console.error('Error creating SSE connection:', error);
        }
    };

    // Disconnect SSE
    const disconnectSSE = () => {
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
        }
        if (reconnectTimeoutRef.current) {
            clearTimeout(reconnectTimeoutRef.current);
            reconnectTimeoutRef.current = null;
        }
    };

    // Start pipeline
    const startPipeline = async () => {
        if (!user?.user_id) return;
        
        setIsLoading(true);
        try {
            const response = await axiosInstance.post('/pipeline/start', {
                userId: user.user_id
            });
            
            if (response.data.status === 200) {
                setPipelineId(response.data.pipeline_id);
                setPipelineStatus('running');
                setProgressPercentage(0);
                setStepDescription('Pipeline starting...');
                setErrorMessage(null);
                
                // Start SSE connection
                connectSSE();
                
                return { success: true };
            }
        } catch (error) {
            console.error('Error starting pipeline:', error);
            setErrorMessage(error.response?.data?.detail || 'Failed to start pipeline');
            return { success: false, error: error.response?.data?.detail };
        } finally {
            setIsLoading(false);
        }
    };

    // Cancel pipeline
    const cancelPipeline = async () => {
        if (!user?.user_id) return;
        
        setIsLoading(true);
        try {
            const response = await axiosInstance.post('/pipeline/cancel', {
                userId: user.user_id
            });
            
            if (response.data.status === 200) {
                setPipelineStatus('cancelled');
                setStepDescription('Pipeline cancelled by user');
                disconnectSSE();
                
                return { success: true };
            }
        } catch (error) {
            console.error('Error cancelling pipeline:', error);
            return { success: false, error: error.response?.data?.detail };
        } finally {
            setIsLoading(false);
        }
    };

    // Retry pipeline
    const retryPipeline = async () => {
        if (!user?.user_id) return;
        
        setIsLoading(true);
        try {
            const response = await axiosInstance.post('/pipeline/retry', {
                userId: user.user_id
            });
            
            if (response.data.status === 200) {
                setPipelineId(response.data.pipeline_id);
                setPipelineStatus('running');
                setProgressPercentage(0);
                setStepDescription('Pipeline restarting...');
                setErrorMessage(null);
                
                // Start SSE connection
                connectSSE();
                
                return { success: true };
            }
        } catch (error) {
            console.error('Error retrying pipeline:', error);
            setErrorMessage(error.response?.data?.detail || 'Failed to retry pipeline');
            return { success: false, error: error.response?.data?.detail };
        } finally {
            setIsLoading(false);
        }
    };

    // Initialize pipeline status on mount
    useEffect(() => {
        if (user?.user_id) {
            fetchPipelineStatus();
        }
        
        // Cleanup on unmount
        return () => {
            disconnectSSE();
        };
    }, [user?.user_id]);

    const value = {
        pipelineId,
        pipelineStatus,
        currentPhase,
        progressPercentage,
        stepDescription,
        errorMessage,
        isLoading,
        startPipeline,
        cancelPipeline,
        retryPipeline,
        refreshStatus: fetchPipelineStatus
    };

    return (
        <PipelineContext.Provider value={value}>
            {children}
        </PipelineContext.Provider>
    );
};
