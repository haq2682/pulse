import { useState, useEffect, useCallback, useRef } from 'react';

/**
 * Custom hook for real-time analytics updates via WebSocket
 * Connects to backend analytics WebSocket endpoint and listens for parquet file updates
 */
export const useAnalyticsWebSocket = (businessId) => {
    const [updates, setUpdates] = useState([]);
    const [isConnected, setIsConnected] = useState(false);
    const [lastUpdate, setLastUpdate] = useState(null);
    const [error, setError] = useState(null);
    
    const wsRef = useRef(null);
    const reconnectTimeoutRef = useRef(null);
    const reconnectAttemptsRef = useRef(0);
    const pingIntervalRef = useRef(null);
    const shouldReconnectRef = useRef(true);
    const isConnectingRef = useRef(false);
    
    const MAX_RECONNECT_ATTEMPTS = 5;
    const RECONNECT_DELAY = 3000; // 3 seconds
    const PING_INTERVAL = 30000; // 30 seconds
    
    // Get WebSocket URL
    const getWebSocketUrl = useCallback((businessId) => {
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const wsProtocol = apiUrl.startsWith('https') ? 'wss' : 'ws';
        const wsHost = apiUrl.replace(/^https?:\/\//, '');
        return `${wsProtocol}://${wsHost}/analytics/ws/${businessId}`;
    }, []);
    
    // Connect to WebSocket
    const connect = useCallback(() => {
        if (!businessId) {
            import.meta.env.DEV && console.log('No business ID provided, skipping analytics WebSocket connection');
            return;
        }
        
        // Prevent duplicate connections
        if (isConnectingRef.current || 
            (wsRef.current && wsRef.current.readyState === WebSocket.OPEN)) {
            import.meta.env.DEV && console.log(`Analytics WebSocket already connected or connecting for business ${businessId}`);
            return;
        }
        
        isConnectingRef.current = true;
        shouldReconnectRef.current = true;
        
        try {
            const wsUrl = getWebSocketUrl(businessId);
            import.meta.env.DEV && console.log(`Connecting to Analytics WebSocket: ${wsUrl}`);
            
            const ws = new WebSocket(wsUrl);
            wsRef.current = ws;
            
            ws.onopen = () => {
                import.meta.env.DEV && console.log('Analytics WebSocket connected');
                setIsConnected(true);
                setError(null);
                reconnectAttemptsRef.current = 0;
                isConnectingRef.current = false;
                
                // Start keep-alive pings
                if (pingIntervalRef.current) {
                    clearInterval(pingIntervalRef.current);
                }
                pingIntervalRef.current = setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send('ping');
                    }
                }, PING_INTERVAL);
            };
            
            ws.onmessage = (event) => {
                try {
                    // Handle pong response
                    if (event.data === 'pong') {
                        return;
                    }
                    
                    const data = JSON.parse(event.data);
                    import.meta.env.DEV && console.log('Analytics update received:', data);
                    
                    // Handle different event types
                    if (data.event === 'connected') {
                        import.meta.env.DEV && console.log('Analytics WebSocket connection confirmed');
                        return;
                    }
                    
                    if (data.event === 'analytics_updated') {
                        // Add timestamp if not present
                        const update = {
                            ...data,
                            receivedAt: new Date().toISOString()
                        };
                        
                        setLastUpdate(update);
                        setUpdates(prev => [...prev, update]);
                    }
                } catch (err) {
                    console.error('Error parsing analytics WebSocket message:', err);
                }
            };
            
            ws.onerror = (error) => {
                console.error('Analytics WebSocket error:', error);
                setError('Connection error');
                isConnectingRef.current = false;
            };
            
            ws.onclose = () => {
                import.meta.env.DEV && console.log('Analytics WebSocket disconnected');
                setIsConnected(false);
                wsRef.current = null;
                isConnectingRef.current = false;
                
                // Clear ping interval
                if (pingIntervalRef.current) {
                    clearInterval(pingIntervalRef.current);
                    pingIntervalRef.current = null;
                }
                
                // Attempt to reconnect
                if (shouldReconnectRef.current && reconnectAttemptsRef.current < MAX_RECONNECT_ATTEMPTS) {
                    reconnectAttemptsRef.current++;
                    import.meta.env.DEV && console.log(`Reconnecting analytics WebSocket... (attempt ${reconnectAttemptsRef.current}/${MAX_RECONNECT_ATTEMPTS})`);
                    
                    reconnectTimeoutRef.current = setTimeout(() => {
                        connect();
                    }, RECONNECT_DELAY);
                }
            };
            
        } catch (err) {
            console.error('Error creating analytics WebSocket connection:', err);
            setError('Failed to connect');
            isConnectingRef.current = false;
        }
    }, [businessId, getWebSocketUrl]);
    
    // Disconnect from WebSocket
    const disconnect = useCallback(() => {
        shouldReconnectRef.current = false;
        
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
    
    // Clear updates history
    const clearUpdates = useCallback(() => {
        setUpdates([]);
        setLastUpdate(null);
    }, []);
    
    // Manual trigger to force check for updates
    const triggerRefresh = useCallback(async () => {
        if (!businessId) return;
        
        try {
            const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
            const response = await fetch(`${apiUrl}/analytics/trigger-update/${businessId}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include'
            });
            
            if (response.ok) {
                import.meta.env.DEV && console.log('Manual refresh triggered');
            }
        } catch (err) {
            console.error('Error triggering manual refresh:', err);
        }
    }, [businessId]);
    
    // Connect on mount, disconnect on unmount
    useEffect(() => {
        connect();
        
        return () => {
            disconnect();
        };
    }, [connect, disconnect]);
    
    return {
        updates,
        isConnected,
        lastUpdate,
        error,
        clearUpdates,
        triggerRefresh,
        reconnect: connect,
        disconnect
    };
};

export default useAnalyticsWebSocket;
