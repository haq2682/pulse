import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react';
import { useAuth } from './AuthContext';

const PipelineProgressContext = createContext(null);

export const PipelineProgressProvider = ({ children }) => {
    const { user } = useAuth();
    const [pipelineStatus, setPipelineStatus] = useState(null);
    const [isConnected, setIsConnected] = useState(false);
    const [error, setError] = useState(null);
    
    const wsRef = useRef(null);
    const reconnectTimeoutRef = useRef(null);
    const reconnectAttemptsRef = useRef(0);
    const pingIntervalRef = useRef(null);
    const shouldReconnectRef = useRef(true);
    const currentBusinessIdRef = useRef(null);
    const isConnectingRef = useRef(false);
    const MAX_RECONNECT_ATTEMPTS = 5;
    const RECONNECT_DELAY = 3000;
    
    // Get API URL from environment
    const getWebSocketUrl = useCallback((businessId) => {
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const wsProtocol = apiUrl.startsWith('https') ? 'wss' : 'ws';
        const wsHost = apiUrl.replace(/^https?:\/\//, '');
        return `${wsProtocol}://${wsHost}/pipeline/ws/${businessId}`;
    }, []);
    
    const connectWebSocket = useCallback((businessId) => {
        if (!businessId) return;
        
        // Prevent duplicate connections for the same business
        if (isConnectingRef.current || 
            (wsRef.current && wsRef.current.readyState === WebSocket.OPEN && currentBusinessIdRef.current === businessId)) {
            import.meta.env.DEV && console.log(`WebSocket already connected or connecting for business ${businessId}`);
            return;
        }
        
        // Close existing connection if switching to a different business
        if (wsRef.current && currentBusinessIdRef.current !== businessId) {
            import.meta.env.DEV && console.log(`Closing existing connection for business ${currentBusinessIdRef.current}`);
            wsRef.current.close();
            wsRef.current = null;
        }
        
        isConnectingRef.current = true;
        currentBusinessIdRef.current = businessId;
        shouldReconnectRef.current = true;
        
        try {
            const wsUrl = getWebSocketUrl(businessId);
            import.meta.env.DEV && console.log(`Connecting to WebSocket: ${wsUrl}`);
            
            const ws = new WebSocket(wsUrl);
            wsRef.current = ws;
            
            ws.onopen = () => {
                import.meta.env.DEV && console.log('WebSocket connected');
                setIsConnected(true);
                setError(null);
                reconnectAttemptsRef.current = 0;
                isConnectingRef.current = false;
                
                // Send ping to keep connection alive
                if (pingIntervalRef.current) {
                    clearInterval(pingIntervalRef.current);
                }
                pingIntervalRef.current = setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send('ping');
                    }
                }, 30000); // Ping every 30 seconds
            };
            
            ws.onmessage = (event) => {
                try {
                    // Handle pong response
                    if (event.data === 'pong') {
                        return;
                    }
                    
                    const data = JSON.parse(event.data);
                    import.meta.env.DEV && console.log('Pipeline update received:', data);
                    setPipelineStatus(data);
                } catch (err) {
                    console.error('Error parsing WebSocket message:', err);
                }
            };
            
            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                setError('Connection error');
                isConnectingRef.current = false;
            };
            
            ws.onclose = () => {
                import.meta.env.DEV && console.log('WebSocket disconnected');
                setIsConnected(false);
                wsRef.current = null;
                isConnectingRef.current = false;
                
                // Clear ping interval
                if (pingIntervalRef.current) {
                    clearInterval(pingIntervalRef.current);
                    pingIntervalRef.current = null;
                }
                
                // Attempt to reconnect if allowed and still on the same business
                if (shouldReconnectRef.current && 
                    reconnectAttemptsRef.current < MAX_RECONNECT_ATTEMPTS &&
                    currentBusinessIdRef.current === businessId) {
                    reconnectAttemptsRef.current++;
                    import.meta.env.DEV && console.log(`Reconnecting... (attempt ${reconnectAttemptsRef.current}/${MAX_RECONNECT_ATTEMPTS})`);
                    
                    reconnectTimeoutRef.current = setTimeout(() => {
                        connectWebSocket(businessId);
                    }, RECONNECT_DELAY);
                }
            };
            
        } catch (err) {
            console.error('Error creating WebSocket connection:', err);
            setError('Failed to connect');
            isConnectingRef.current = false;
        }
    }, [getWebSocketUrl]);
    
    const disconnectWebSocket = useCallback(() => {
        // Prevent auto-reconnection
        shouldReconnectRef.current = false;
        currentBusinessIdRef.current = null;
        
        if (reconnectTimeoutRef.current) {
            clearTimeout(reconnectTimeoutRef.current);
            reconnectTimeoutRef.current = null;
        }
        
        if (pingIntervalRef.current) {
            clearInterval(pingIntervalRef.current);
            pingIntervalRef.current = null;
        }
        
        if (wsRef.current) {
            wsRef.current.close();
            wsRef.current = null;
        }
        
        setIsConnected(false);
        isConnectingRef.current = false;
    }, []);
    
    // Fetch current pipeline status from REST API
    const fetchPipelineStatus = useCallback(async (businessId) => {
        if (!businessId) return;
        setPipelineStatus('loading');
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/pipeline/status?business_id=${businessId}`);
            
            if (response.ok) {
                const result = await response.json();
                if (result.data) {
                    setPipelineStatus(result.data);
                } else if (result.pipeline_status === 'not_started') {
                    setPipelineStatus(null);
                }
            }
        } catch (err) {
            console.error('Error fetching pipeline status:', err);
            setPipelineStatus(null);
        }
    }, []);
    
    // Start pipeline
    const startPipeline = useCallback(async (businessId) => {
        if (!businessId || !user?.user_id) return { success: false, error: 'Missing required data' };
        
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/pipeline/start`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify({
                    userId: user.user_id,
                    businessId: businessId
                })
            });
            
            const result = await response.json();
            
            if (response.ok) {
                // Connect WebSocket to receive updates
                connectWebSocket(businessId);
                return { success: true, pipelineId: result.pipeline_id };
            } else {
                return { success: false, error: result.detail || 'Failed to start pipeline' };
            }
        } catch (err) {
            console.error('Error starting pipeline:', err);
            return { success: false, error: err.message };
        }
    }, [user, connectWebSocket]);
    
    // Cancel pipeline
    const cancelPipeline = useCallback(async (pipelineId, businessId, cleanupData = true) => {
        if (!pipelineId || !businessId) return { success: false, error: 'Missing required data' };
        
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/pipeline/cancel`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify({
                    pipelineId: pipelineId,
                    businessId: businessId,
                    cleanupData: cleanupData
                })
            });
            
            const result = await response.json();
            
            if (response.ok) {
                setPipelineStatus(null);
                return { success: true };
            } else {
                return { success: false, error: result.detail || 'Failed to cancel pipeline' };
            }
        } catch (err) {
            console.error('Error cancelling pipeline:', err);
            return { success: false, error: err.message };
        }
    }, []);
    
    // Retry pipeline
    const retryPipeline = useCallback(async (businessId) => {
        if (!businessId || !user?.user_id) return { success: false, error: 'Missing required data' };
        
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/pipeline/retry`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify({
                    userId: user.user_id,
                    businessId: businessId
                })
            });
            
            const result = await response.json();
            
            if (response.ok) {
                // Connect WebSocket to receive updates
                connectWebSocket(businessId);
                return { success: true, pipelineId: result.pipeline_id };
            } else {
                return { success: false, error: result.detail || 'Failed to retry pipeline' };
            }
        } catch (err) {
            console.error('Error retrying pipeline:', err);
            return { success: false, error: err.message };
        }
    }, [user, connectWebSocket]);
    
    // Cleanup on unmount
    useEffect(() => {
        return () => {
            disconnectWebSocket();
        };
    }, [disconnectWebSocket]);
    
    const value = {
        pipelineStatus,
        isConnected,
        error,
        connectWebSocket,
        disconnectWebSocket,
        fetchPipelineStatus,
        startPipeline,
        cancelPipeline,
        retryPipeline,
        clearError: () => setError(null)
    };
    
    return (
        <PipelineProgressContext.Provider value={value}>
            {children}
        </PipelineProgressContext.Provider>
    );
};

export const usePipelineProgress = () => {
    const context = useContext(PipelineProgressContext);
    if (!context) {
        throw new Error('usePipelineProgress must be used within a PipelineProgressProvider');
    }
    return context;
};

export default PipelineProgressContext;
