import { useState, useEffect, useCallback, useRef } from 'react';

/**
 * Custom hook for XAI chatbot WebSocket communication.
 * Manages connection, message sending, and reconnection logic.
 */
export const useXAIWebSocket = (businessId) => {
    const [messages, setMessages] = useState([]);
    const [isConnected, setIsConnected] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [conversationId, setConversationId] = useState(null);
    const [error, setError] = useState(null);

    const wsRef = useRef(null);
    const reconnectTimeoutRef = useRef(null);
    const reconnectAttemptsRef = useRef(0);
    const pingIntervalRef = useRef(null);
    const shouldReconnectRef = useRef(true);
    const isConnectingRef = useRef(false);

    const MAX_RECONNECT_ATTEMPTS = 5;
    const RECONNECT_DELAY = 3000;
    const PING_INTERVAL = 30000;
    const connectRef = useRef(null);

    const getWebSocketUrl = useCallback(() => {
        const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000';
        const wsProtocol = apiUrl.startsWith('https') ? 'wss' : 'ws';
        const wsHost = apiUrl.replace(/^https?:\/\//, '');
        return `${wsProtocol}://${wsHost}/xai/ws/${businessId}`;
    }, [businessId]);

    const connect = useCallback(() => {
        if (!businessId) return;
        if (isConnectingRef.current || (wsRef.current && wsRef.current.readyState === WebSocket.OPEN)) {
            return;
        }
        isConnectingRef.current = true;
        shouldReconnectRef.current = true;

        try {
            const wsUrl = getWebSocketUrl();
            const ws = new WebSocket(wsUrl);
            wsRef.current = ws;

            ws.onopen = () => {
                setIsConnected(true);
                setError(null);
                reconnectAttemptsRef.current = 0;
                isConnectingRef.current = false;

                if (pingIntervalRef.current) clearInterval(pingIntervalRef.current);
                pingIntervalRef.current = setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send('ping');
                    }
                }, PING_INTERVAL);
            };

            ws.onmessage = (event) => {
                if (event.data === 'pong') return;

                try {
                    const data = JSON.parse(event.data);

                    switch (data.type) {
                        case 'conversation_created':
                            setConversationId(data.conversationId);
                            break;

                        case 'user_echo':
                            setMessages(prev => [...prev, {
                                id: data.messageId,
                                role: 'user',
                                content: data.content,
                                conversationId: data.conversationId,
                                createdAt: data.createdAt,
                            }]);
                            break;

                        case 'assistant':
                            setIsLoading(false);
                            setMessages(prev => [...prev, {
                                id: data.messageId,
                                role: 'assistant',
                                content: data.content,
                                context: data.context,
                                conversationId: data.conversationId,
                                createdAt: data.createdAt,
                            }]);
                            break;

                        case 'notification':
                            if (data.severity === 'error' || data.severity === 'warning') {
                                setIsLoading(false);
                            }
                            setMessages(prev => [...prev, {
                                id: `notif-${Date.now()}`,
                                role: 'notification',
                                content: data.content,
                                severity: data.severity || 'info',
                                conversationId: data.conversationId,
                                createdAt: new Date().toISOString(),
                            }]);
                            break;

                        default:
                            break;
                    }
                } catch (err) {
                    console.error('[XAI WS] Parse error:', err);
                }
            };

            ws.onerror = (event) => {
                console.error('[XAI WS] Error:', event);
                setError('WebSocket connection error');
                isConnectingRef.current = false;
            };

            ws.onclose = () => {
                setIsConnected(false);
                setIsLoading(false);
                isConnectingRef.current = false;

                if (pingIntervalRef.current) {
                    clearInterval(pingIntervalRef.current);
                    pingIntervalRef.current = null;
                }

                if (shouldReconnectRef.current && reconnectAttemptsRef.current < MAX_RECONNECT_ATTEMPTS) {
                    reconnectAttemptsRef.current++;
                    reconnectTimeoutRef.current = setTimeout(() => connectRef.current?.(), RECONNECT_DELAY);
                }
            };
        } catch (err) {
            console.error('[XAI WS] Connection error:', err);
            isConnectingRef.current = false;
        }
    }, [businessId, getWebSocketUrl]);

    useEffect(() => {
        connectRef.current = connect;
    }, [connect]);

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
    }, []);

    const sendQuery = useCallback((content, convId = null) => {
        if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
            setError('Not connected. Please wait...');
            return;
        }

        setIsLoading(true);
        setError(null);

        wsRef.current.send(JSON.stringify({
            type: 'query',
            content,
            conversationId: convId || conversationId,
        }));
    }, [conversationId]);

    // Load an existing conversation's messages
    const loadConversation = useCallback((convId, existingMessages) => {
        setConversationId(convId);
        setMessages(existingMessages || []);
    }, []);

    // Start fresh chat
    const newChat = useCallback(() => {
        setConversationId(null);
        setMessages([]);
        setIsLoading(false);
        setError(null);
    }, []);

    // Connect on mount
    useEffect(() => {
        connect();
        return () => disconnect();
    }, [connect, disconnect]);

    return {
        messages,
        isConnected,
        isLoading,
        conversationId,
        error,
        sendQuery,
        loadConversation,
        newChat,
        setMessages,
    };
};
