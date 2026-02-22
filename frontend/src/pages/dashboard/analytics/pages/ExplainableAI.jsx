import React, { useState, useEffect, useRef, useCallback, startTransition } from 'react';
import { useParams } from 'react-router-dom';
import { useAuth } from '@/context/AuthContext';
import { useXAIWebSocket } from '@/hooks/useXAIWebSocket';
import axiosInstance from '@/services/api/axiosInstance';

// ─── Simple Markdown Renderer ──────────────────────────────────────────────
// Converts markdown subset (headers, bold, italic, bullets, code) to HTML.
const renderMarkdown = (text) => {
    if (!text) return '';
    let html = text
        // Code blocks
        .replace(/```(\w*)\n([\s\S]*?)```/g, '<pre class="xai-code-block"><code>$2</code></pre>')
        // Inline code
        .replace(/`([^`]+)`/g, '<code class="xai-inline-code">$1</code>')
        // Headers
        .replace(/^#### (.+)$/gm, '<h4 class="xai-h4">$1</h4>')
        .replace(/^### (.+)$/gm, '<h3 class="xai-h3">$1</h3>')
        .replace(/^## (.+)$/gm, '<h2 class="xai-h2">$1</h2>')
        .replace(/^# (.+)$/gm, '<h1 class="xai-h1">$1</h1>')
        // Bold + Italic
        .replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>')
        // Bold
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        // Italic
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        // Unordered lists
        .replace(/^[-*] (.+)$/gm, '<li class="xai-li">$1</li>')
        // Numbered lists
        .replace(/^\d+\. (.+)$/gm, '<li class="xai-li-num">$1</li>')
        // Line breaks (double newline = paragraph)
        .replace(/\n\n/g, '</p><p class="xai-p">')
        // Single newline = br
        .replace(/\n/g, '<br/>');

    // Wrap consecutive <li> tags in <ul>
    html = html.replace(/((?:<li class="xai-li">.*?<\/li>\s*)+)/g, '<ul class="xai-ul">$1</ul>');
    html = html.replace(/((?:<li class="xai-li-num">.*?<\/li>\s*)+)/g, '<ol class="xai-ol">$1</ol>');

    return `<p class="xai-p">${html}</p>`;
};


// ─── Chat Message Component ────────────────────────────────────────────────
const ChatMessage = ({ message }) => {
    if (message.role === 'notification') {
        const isError = message.severity === 'error';
        const isWarning = message.severity === 'warning';
        return (
            <div className="flex justify-center my-2 px-4">
                <div className={`
                    text-xs px-4 py-2 rounded-full max-w-lg text-center
                    ${isError ? 'bg-red-50 text-red-600 border border-red-200' :
                      isWarning ? 'bg-yellow-50 text-yellow-700 border border-yellow-200' :
                      'bg-gray-50 text-gray-500 border border-gray-200'}
                `}>
                    {message.content}
                </div>
            </div>
        );
    }

    const isUser = message.role === 'user';

    return (
        <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} mb-4 px-4`}>
            <div className={`
                max-w-[80%] md:max-w-[70%] rounded-2xl px-4 py-3
                ${isUser
                    ? 'bg-gradient-to-r from-emerald-500 to-teal-400 text-white rounded-br-md'
                    : 'bg-white border border-emerald-500 text-emerald-500 rounded-bl-md shadow-sm'}
            `}>
                {isUser ? (
                    <p className="text-sm leading-relaxed whitespace-pre-wrap">{message.content}</p>
                ) : (
                    <div
                        className="text-sm leading-relaxed xai-markdown-content"
                        dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }}
                    />
                )}
                <div className={`text-[10px] mt-1 ${isUser ? 'text-emerald-100 text-right' : 'text-gray-400'}`}>
                    {message.createdAt ? new Date(message.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : ''}
                </div>
            </div>
        </div>
    );
};


// ─── Typing Indicator ──────────────────────────────────────────────────────
const TypingIndicator = () => (
    <div className="flex justify-start mb-4 px-4">
        <div className="bg-white border border-gray-200 rounded-2xl rounded-bl-md px-4 py-3 shadow-sm">
            <div className="flex items-center gap-1.5">
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
            </div>
        </div>
    </div>
);


// ─── Chat History Sidebar ──────────────────────────────────────────────────
const ChatHistorySidebar = ({ isOpen, onClose, conversations, activeConversationId, onSelectConversation, onNewChat, onDeleteConversation }) => {
    return (
        <>
            {/* Mobile overlay */}
            {isOpen && (
                <div className="fixed inset-0 bg-black/40 z-40 xl:hidden" onClick={onClose} />
            )}

            <aside className={`
                fixed xl:relative top-0 right-0 h-full w-72
                bg-white border-l border-gray-200
                transform transition-transform duration-300 ease-in-out z-50
                ${isOpen ? 'translate-x-0' : 'translate-x-full xl:translate-x-0'}
                flex flex-col
                xl:w-72 xl:min-w-[18rem] xl:flex
            `}>
                {/* Header */}
                <div className="p-4 border-b border-gray-200 flex items-center justify-between">
                    <h3 className="text-sm font-semibold text-gray-700">Chat History</h3>
                    <div className="flex items-center gap-2">
                        <button
                            onClick={onNewChat}
                            className="p-1.5 hover:bg-gray-100 rounded-lg transition-colors"
                            title="New chat"
                        >
                            <i className="pi pi-plus text-gray-600 text-sm" />
                        </button>
                        <button
                            onClick={onClose}
                            className="xl:hidden p-1.5 hover:bg-gray-100 rounded-lg transition-colors"
                        >
                            <i className="pi pi-times text-gray-600 text-sm" />
                        </button>
                    </div>
                </div>

                {/* Conversation list */}
                <div className="flex-1 overflow-y-auto py-2">
                    {conversations.length === 0 ? (
                        <div className="px-4 py-8 text-center text-gray-400 text-xs">
                            No conversations yet
                        </div>
                    ) : (
                        conversations.map((conv) => (
                            <div
                                key={conv.conversationId}
                                className={`
                                    group flex items-center gap-2 mx-2 px-3 py-2.5 rounded-lg cursor-pointer
                                    transition-colors duration-150
                                    ${conv.conversationId === activeConversationId
                                        ? 'bg-emerald-50 border border-emerald-200'
                                        : 'hover:bg-gray-50'}
                                `}
                                onClick={() => onSelectConversation(conv.conversationId)}
                            >
                                <i className={`pi pi-comments text-xs ${
                                    conv.conversationId === activeConversationId ? 'text-emerald-500' : 'text-gray-400'
                                }`} />
                                <span className="flex-1 text-xs text-gray-700 truncate">{conv.title}</span>
                                <button
                                    className="opacity-0 group-hover:opacity-100 p-1 hover:bg-red-50 rounded transition-all"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        onDeleteConversation(conv.conversationId);
                                    }}
                                    title="Delete conversation"
                                >
                                    <i className="pi pi-trash text-[10px] text-red-400" />
                                </button>
                            </div>
                        ))
                    )}
                </div>
            </aside>
        </>
    );
};


// ─── Main ExplainableAI Component ──────────────────────────────────────────
const ExplainableAI = () => {
    const { businessId } = useParams();
    const { user } = useAuth();
    const [inputValue, setInputValue] = useState('');
    const [sidebarOpen, setSidebarOpen] = useState(false);
    const [conversations, setConversations] = useState([]);
    const messagesEndRef = useRef(null);
    const inputRef = useRef(null);

    const {
        messages,
        isConnected,
        isLoading,
        conversationId,
        error,
        sendQuery,
        loadConversation,
        newChat,
    } = useXAIWebSocket(businessId);

    const userId = user?.user_id;

    // Auto-scroll to bottom on new messages
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages, isLoading]);

    // Load conversations
    const fetchConversations = useCallback(async () => {
        if (!userId || !businessId) return;
        try {
            const res = await axiosInstance.get(`/xai/conversations/${businessId}`, {
                params: { userId }
            });
            startTransition(() => {
                setConversations(res.data.conversations || []);
            });
        } catch (err) {
            console.error('Failed to load conversations:', err);
        }
    }, [userId, businessId]);

    useEffect(() => {
        fetchConversations();
    }, [fetchConversations]);

    // Refresh conversations after getting a new response
    useEffect(() => {
        if (conversationId) {
            fetchConversations();
        }
    }, [messages.length, conversationId, fetchConversations]);

    // Select a conversation from history
    const handleSelectConversation = useCallback(async (convId) => {
        try {
            const res = await axiosInstance.get(`/xai/conversation/${convId}/messages`, {
                params: { userId }
            });
            const msgs = (res.data.messages || []).map(m => ({
                id: m.messageId,
                role: m.role,
                content: m.content,
                severity: m.severity,
                createdAt: m.createdAt,
                metadata: m.metadata,
            }));
            loadConversation(convId, msgs);
            setSidebarOpen(false);
        } catch (err) {
            console.error('Failed to load messages:', err);
        }
    }, [userId, loadConversation]);

    // Delete conversation
    const handleDeleteConversation = useCallback(async (convId) => {
        try {
            await axiosInstance.delete(`/xai/conversation/${convId}`, {
                params: { userId }
            });
            setConversations(prev => prev.filter(c => c.conversationId !== convId));
            if (convId === conversationId) {
                newChat();
            }
        } catch (err) {
            console.error('Failed to delete conversation:', err);
        }
    }, [userId, conversationId, newChat]);

    // New chat
    const handleNewChat = useCallback(() => {
        newChat();
        setSidebarOpen(false);
    }, [newChat]);

    // Send message
    const handleSend = useCallback(() => {
        const trimmed = inputValue.trim();
        if (!trimmed || isLoading) return;
        sendQuery(trimmed);
        setInputValue('');
        inputRef.current?.focus();
    }, [inputValue, isLoading, sendQuery]);

    // Handle Enter key
    const handleKeyDown = useCallback((e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    }, [handleSend]);

    const hasMessages = messages.length > 0;

    return (
        <div className="flex h-full overflow-hidden relative">
            {/* ── Main Chat Area ──────────────────────────────────────── */}
            <div className="flex-1 flex flex-col min-w-0">
                {/* Top bar */}
                <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 bg-white">
                    <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded-full bg-gradient-to-r from-emerald-500 to-teal-300 flex items-center justify-center">
                            <i className="pi pi-sparkles text-white text-sm" />
                        </div>
                        <div>
                            <h2 className="text-sm font-semibold text-gray-800">Pulse AI</h2>
                            <span className={`text-[10px] ${isConnected ? 'text-green-500' : 'text-gray-400'}`}>
                                {isConnected ? '● Connected' : '○ Disconnected'}
                            </span>
                        </div>
                    </div>
                    <button
                        className="xl:hidden p-2 hover:bg-gray-100 rounded-lg transition-colors"
                        onClick={() => setSidebarOpen(true)}
                        title="Chat history"
                    >
                        <i className="pi pi-history text-gray-600" />
                    </button>
                </div>

                {/* Messages area */}
                <div className="flex-1 overflow-y-auto py-4 bg-gray-50">
                    {!hasMessages && !isLoading ? (
                        <div className="h-full flex flex-col items-center justify-center px-4">
                            <div className="w-16 h-16 rounded-2xl bg-gradient-to-r from-emerald-500 to-teal-300 flex items-center justify-center mb-4">
                                <i className="pi pi-sparkles text-white text-2xl" />
                            </div>
                            <h3 className="text-lg font-semibold text-gray-700 mb-1">Ask Pulse AI</h3>
                            <p className="text-sm text-gray-500 text-center max-w-md mb-6">
                                Ask questions about your analytics, insights, forecasts, and ML predictions. I&apos;ll analyze your data and provide actionable answers.
                            </p>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-w-lg w-full">
                                {[
                                    "How is my business performing this month?",
                                    "Which customers are at risk of churning?",
                                    "What are my best-selling products?",
                                    "Show me the revenue forecast",
                                ].map((suggestion, i) => (
                                    <button
                                        key={i}
                                        className="text-left text-xs px-3 py-2.5 bg-white border border-gray-200 rounded-xl hover:border-emerald-300 hover:bg-emerald-50 transition-colors text-gray-600"
                                        onClick={() => {
                                            setInputValue(suggestion);
                                            inputRef.current?.focus();
                                        }}
                                    >
                                        {suggestion}
                                    </button>
                                ))}
                            </div>
                        </div>
                    ) : (
                        <>
                            {messages.map((msg, idx) => (
                                <ChatMessage key={msg.id || idx} message={msg} />
                            ))}
                            {isLoading && <TypingIndicator />}
                            <div ref={messagesEndRef} />
                        </>
                    )}
                </div>

                {/* Error bar */}
                {error && (
                    <div className="px-4 py-2 bg-red-50 border-t border-red-200 text-red-600 text-xs">
                        {error}
                    </div>
                )}

                {/* Input area */}
                <div className="border-t border-gray-200 bg-white p-3 sm:p-4">
                    <div className="flex items-center gap-2 max-w-4xl mx-auto">
                        <div className="flex-1 relative">
                            <textarea
                                ref={inputRef}
                                value={inputValue}
                                onChange={(e) => setInputValue(e.target.value)}
                                onKeyDown={handleKeyDown}
                                placeholder={isConnected ? "Ask about your analytics..." : "Connecting..."}
                                disabled={!isConnected || isLoading}
                                rows={1}
                                className="w-full resize-none rounded-xl border border-gray-300 px-4 py-2.5 pr-12 text-sm
                                    focus:outline-none focus:ring-2 focus:ring-teal-400 focus:border-transparent
                                    disabled:opacity-50 disabled:cursor-not-allowed
                                    placeholder:text-gray-400 bg-gray-50 focus:bg-white transition-colors text-teal-500"
                                style={{ maxHeight: '120px', minHeight: '42px' }}
                                onInput={(e) => {
                                    e.target.style.height = 'auto';
                                    e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
                                }}
                            />
                        </div>
                        <button
                            onClick={handleSend}
                            disabled={!isConnected || isLoading || !inputValue.trim()}
                            className={`
                                flex items-center justify-center w-10 h-10 mb-1.5 rounded-xl transition-all duration-200
                                ${(!isConnected || isLoading || !inputValue.trim())
                                    ? 'bg-gray-200 text-gray-400 cursor-not-allowed'
                                    : 'bg-gradient-to-r from-emerald-500 to-teal-400 hover:shadow-lg hover:scale-105 active:scale-95'}
                            `}
                            title="Send message"
                        >
                            <i className={`pi ${isLoading ? 'pi-spin pi-spinner' : 'pi-send'} text-sm`} />
                        </button>
                    </div>
                </div>
            </div>

            {/* ── Right Sidebar (Chat History) ───────────────────────── */}
            <div className="hidden xl:block">
                <ChatHistorySidebar
                    isOpen={true}
                    onClose={() => {}}
                    conversations={conversations}
                    activeConversationId={conversationId}
                    onSelectConversation={handleSelectConversation}
                    onNewChat={handleNewChat}
                    onDeleteConversation={handleDeleteConversation}
                />
            </div>

            {/* Mobile sidebar */}
            <div className="xl:hidden">
                <ChatHistorySidebar
                    isOpen={sidebarOpen}
                    onClose={() => setSidebarOpen(false)}
                    conversations={conversations}
                    activeConversationId={conversationId}
                    onSelectConversation={handleSelectConversation}
                    onNewChat={handleNewChat}
                    onDeleteConversation={handleDeleteConversation}
                />
            </div>

            {/* ── Markdown Styles ─────────────────────────────────────── */}
            <style>{`
                .xai-markdown-content .xai-p { margin-bottom: 0.5rem; }
                .xai-markdown-content .xai-h1 { font-size: 1.25rem; font-weight: 700; margin: 0.75rem 0 0.25rem; }
                .xai-markdown-content .xai-h2 { font-size: 1.1rem; font-weight: 700; margin: 0.75rem 0 0.25rem; }
                .xai-markdown-content .xai-h3 { font-size: 1rem; font-weight: 600; margin: 0.5rem 0 0.25rem; }
                .xai-markdown-content .xai-h4 { font-size: 0.9rem; font-weight: 600; margin: 0.5rem 0 0.25rem; }
                .xai-markdown-content .xai-ul { list-style: disc; padding-left: 1.25rem; margin: 0.25rem 0; }
                .xai-markdown-content .xai-ol { list-style: decimal; padding-left: 1.25rem; margin: 0.25rem 0; }
                .xai-markdown-content .xai-li, .xai-markdown-content .xai-li-num { margin: 0.15rem 0; }
                .xai-markdown-content .xai-code-block {
                    background: #1e293b; color: #e2e8f0; padding: 0.75rem 1rem;
                    border-radius: 0.5rem; overflow-x: auto; font-size: 0.8rem;
                    margin: 0.5rem 0; white-space: pre-wrap;
                }
                .xai-markdown-content .xai-inline-code {
                    background: #f1f5f9; color: #6d28d9; padding: 0.1rem 0.35rem;
                    border-radius: 0.25rem; font-size: 0.85em;
                }
                .xai-markdown-content strong { font-weight: 600; }
                .xai-markdown-content em { font-style: italic; }
            `}</style>
        </div>
    );
};

export default ExplainableAI;
