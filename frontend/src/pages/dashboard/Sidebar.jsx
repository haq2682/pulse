import React from 'react';
import { useNavigate, useLocation } from 'react-router';
import Heading from '@/components/global/Typography/Heading';

const Sidebar = ({ isOpen, onClose }) => {
    const navigate = useNavigate();
    const location = useLocation();

    const menuItems = [
        {
            icon: 'pi pi-home',
            label: 'Overview',
            path: '/analytics',
            activeIcon: 'pi pi-home'
        },
        {
            icon: 'pi pi-chart-line',
            label: 'Revenue Analytics',
            path: '/analytics/revenue-analytics',
            activeIcon: 'pi pi-chart-line'
        },
        {
            icon: 'pi pi-users',
            label: 'Customer Insights',
            path: '/analytics/customer-insights',
            activeIcon: 'pi pi-users'
        },
        {
            icon: 'pi pi-box',
            label: 'Product Performance',
            path: '/analytics/product-performance',
            activeIcon: 'pi pi-box'
        },
        {
            icon: 'pi pi-chart-bar',
            label: 'Sales Forecasting',
            path: '/analytics/sales-forecasting',
            activeIcon: 'pi pi-chart-bar'
        },
        {
            icon: 'pi pi-globe',
            label: 'Geographic Analysis',
            path: '/analytics/geographic-analysis',
            activeIcon: 'pi pi-globe'
        },
        {
            icon: 'pi pi-inbox',
            label: 'Inventory Management',
            path: '/analytics/inventory-management',
            activeIcon: 'pi pi-inbox'
        },
        {
            icon: 'pi pi-sparkles',
            label: 'AI Predictions',
            path: '/analytics/ai-predictions',
            activeIcon: 'pi pi-sparkles'
        },
    ];

    const handleNavigation = (path) => {
        navigate(path);
        if (onClose) onClose();
    };

    const isActive = (path) => location.pathname === path;

    return (
        <>
            {/* Mobile Overlay */}
            {isOpen && (
                <div
                    className="fixed inset-0 bg-black bg-opacity-50 z-40 lg: hidden"
                    onClick={onClose}
                />
            )}

            {/* Sidebar */}
            <aside
                className={`
                    fixed lg:sticky top-0 left-0 h-screen w-64 
                    bg-white border-r border-gray-200
                    transform transition-transform duration-300 ease-in-out z-50
                    ${isOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}
                    flex flex-col
                `}
            >
                {/* Logo Section */}
                <div className="p-5.5 border-b border-gray-200 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <Heading level={3} gradient={true} className="text-xl md:text-2xl m-0">
                            Pulse Analytics
                        </Heading>
                    </div>
                    <button
                        onClick={onClose}
                        className="lg:hidden p-2 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        <i className="pi pi-times text-gray-600"></i>
                    </button>
                </div>

                {/* Navigation Menu */}
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
                                    <i className={`${item.icon} text-lg`}></i>
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