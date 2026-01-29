import React from 'react';
import { useNavigate, useLocation } from 'react-router';
import Heading from '@/components/global/Typography/Heading';

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const Sidebar = ({ isOpen, onClose }) => {
    const navigate = useNavigate();
    const location = useLocation();

    const pathSegments = location.pathname.split('/').filter(Boolean);

    // Example paths:
    // /analytics
    // /analytics/<uuid>/revenue-analytics
    // /analytics/product-performance   <-- NOT a UUID

    const analyticsIndex = pathSegments.indexOf('analytics');
    const possibleId = pathSegments[analyticsIndex + 1];

    const businessId = UUID_REGEX.test(possibleId) ? possibleId : null;

    const basePath = businessId 
        ? `/analytics/${businessId}` 
        : `/analytics`;

    const menuItems = [
        { icon: 'pi pi-home', label: 'Overview', path: `${basePath}` },
        { icon: 'pi pi-chart-line', label: 'Revenue Analytics', path: `${basePath}/revenue-analytics` },
        { icon: 'pi pi-users', label: 'Customer Insights', path: `${basePath}/customer-insights` },
        { icon: 'pi pi-box', label: 'Product Performance', path: `${basePath}/product-performance` },
        { icon: 'pi pi-chart-bar', label: 'Forecasts & Predictions', path: `${basePath}/forecasts` },
        { icon: 'pi pi-globe', label: 'Geographic Analysis', path: `${basePath}/geographic-analysis` },
        { icon: 'pi pi-inbox', label: 'Inventory Management', path: `${basePath}/inventory-management` },
        { icon: 'pi pi-sparkles', label: 'AI Predictions', path: `${basePath}/ai-predictions` },
    ];

    const handleNavigation = (path) => {
        navigate(path);
        if (onClose) onClose();
    };

    const isActive = (path) => location.pathname === path;

    return (
        <>
            {isOpen && (
                <div className="fixed inset-0 bg-black bg-opacity-50 z-40 lg:hidden" onClick={onClose} />
            )}

            <aside className={`
                fixed lg:sticky top-0 left-0 h-screen w-64 
                bg-white border-r border-gray-200
                transform transition-transform duration-300 ease-in-out z-50
                ${isOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}
                flex flex-col
            `}>
                <div className="p-5.5 border-b border-gray-200 flex items-center justify-between">
                    <Heading level={3} gradient className="text-xl md:text-2xl m-0">
                        Pulse Analytics
                    </Heading>

                    <button onClick={onClose} className="lg:hidden p-2 hover:bg-gray-100 rounded-lg">
                        <i className="pi pi-times text-gray-600"></i>
                    </button>
                </div>

                <nav className="flex-1 overflow-y-auto py-4 px-3">
                    <ul className="space-y-1">
                        {menuItems.map((item, index) => (
                            <li key={index}>
                                <button
                                    onClick={() => handleNavigation(item.path)}
                                    className={`
                                        w-full flex items-center gap-3 px-4 py-3 rounded-lg
                                        text-left font-medium transition-all duration-200
                                        ${isActive(item.path)
                                            ? 'bg-gradient-primary text-white shadow-md'
                                            : 'text-[var(--color-g1)] hover:bg-gray-50'
                                        }
                                    `}
                                >
                                    <i className={`${item.icon} text-lg`} />
                                    <span className="text-sm">{item.label}</span>
                                </button>
                            </li>
                        ))}
                    </ul>
                </nav>
            </aside>
        </>
    );
};

export default Sidebar;
