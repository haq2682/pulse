import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router';
import Heading from '@/components/global/Typography/Heading';

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const Sidebar = ({ isOpen, onClose }) => {
    const navigate = useNavigate();
    const location = useLocation();
    const [expandedSections, setExpandedSections] = useState({});

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
        {
            icon: 'pi pi-home',
            label: 'Executive Overview',
            path: `${basePath}`,
        },
        {
            icon: 'pi pi-users',
            label: 'Customers',
            subItems: [
                { label: 'Overview', path: `${basePath}/customers/overview` },
                { label: 'Segmentation', path: `${basePath}/customers/segmentation` },
                { label: 'Health & Retention', path: `${basePath}/customers/health` },
                { label: 'Value Analysis', path: `${basePath}/customers/value` },
            ]
        },
        {
            icon: 'pi pi-box',
            label: 'Products',
            subItems: [
                { label: 'Performance', path: `${basePath}/products/performance` },
                { label: 'Profitability', path: `${basePath}/products/profitability` },
                { label: 'Engagement', path: `${basePath}/products/engagement` },
                { label: 'Trends', path: `${basePath}/products/trends` },
            ]
        },
        {
            icon: 'pi pi-inbox',
            label: 'Inventory',
            subItems: [
                { label: 'Health', path: `${basePath}/inventory/health` },
                { label: 'Reorder Management', path: `${basePath}/inventory/reorder` },
                { label: 'Efficiency', path: `${basePath}/inventory/efficiency` },
                { label: 'Supplier Inventory', path: `${basePath}/inventory/supplier` },
            ]
        },
        {
            icon: 'pi pi-building',
            label: 'Suppliers',
            subItems: [
                { label: 'Performance', path: `${basePath}/suppliers/performance` },
                { label: 'Operations', path: `${basePath}/suppliers/operations` },
                { label: 'Economics', path: `${basePath}/suppliers/economics` },
            ]
        },
        {
            icon: 'pi pi-megaphone',
            label: 'Marketing',
            subItems: [
                { label: 'Campaigns', path: `${basePath}/marketing/campaigns` },
                { label: 'Attribution', path: `${basePath}/marketing/attribution` },
                { label: 'Channels', path: `${basePath}/marketing/channels` },
            ]
        },
        {
            icon: 'pi pi-shopping-cart',
            label: 'Conversion Funnel',
            subItems: [
                { label: 'Funnel Overview', path: `${basePath}/funnel/overview` },
                { label: 'Cart Analysis', path: `${basePath}/funnel/cart` },
                { label: 'Checkout', path: `${basePath}/funnel/checkout` },
                { label: 'Wishlist', path: `${basePath}/funnel/wishlist` },
            ]
        },
        {
            icon: 'pi pi-credit-card',
            label: 'Payments & Finance',
            subItems: [
                { label: 'Payment Methods', path: `${basePath}/payments/methods` },
                { label: 'Refunds', path: `${basePath}/payments/refunds` },
                { label: 'Financial Metrics', path: `${basePath}/payments/metrics` },
            ]
        },
        {
            icon: 'pi pi-truck',
            label: 'Operations',
            subItems: [
                { label: 'Processing', path: `${basePath}/operations/processing` },
                { label: 'Delivery', path: `${basePath}/operations/delivery` },
                { label: 'Shipping', path: `${basePath}/operations/shipping` },
            ]
        },
        {
            icon: 'pi pi-link',
            label: 'Recommendations',
            subItems: [
                { label: 'Product Affinity', path: `${basePath}/recommendations/product` },
                { label: 'Category Affinity', path: `${basePath}/recommendations/category` },
                { label: 'Coverage', path: `${basePath}/recommendations/coverage` },
            ]
        },
        {
            icon: 'pi pi-star',
            label: 'Reviews & Sentiment',
            subItems: [
                { label: 'Overview', path: `${basePath}/reviews/overview` },
                { label: 'Sentiment', path: `${basePath}/reviews/sentiment` },
                { label: 'Impact', path: `${basePath}/reviews/impact` },
            ]
        },
        {
            icon: 'pi pi-chart-line',
            label: 'Engagement',
            subItems: [
                { label: 'Metrics', path: `${basePath}/engagement/metrics` },
                { label: 'Behavior', path: `${basePath}/engagement/behavior` },
                { label: 'Conversion', path: `${basePath}/engagement/conversion` },
            ]
        },
        {
            icon: 'pi pi-chart-bar',
            label: 'Forecasts & Predictions',
            path: `${basePath}/forecasts`,
        },
        {
            icon: 'pi pi-eye',
            label: 'Explainable AI',
            path: `${basePath}/xai`,
        },
        {
            icon: 'pi pi-download',
            label: 'Export Analytics',
            path: `${basePath}/export`,
        },
    ];

    const handleNavigation = (path) => {
        navigate(path);
        if (onClose) onClose();
    };

    const toggleSection = (label) => {
        setExpandedSections(prev => ({
            ...prev,
            [label]: !prev[label]
        }));
    };

    const isActive = (path) => location.pathname === path;

    const isParentActive = (item) => {
        if (!item.subItems) return false;
        return item.subItems.some(subItem => location.pathname === subItem.path);
    };

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
                                {/* Parent Item */}
                                {item.subItems ? (
                                    <div>
                                        <button
                                            onClick={() => toggleSection(item.label)}
                                            className={`
                                                w-full flex items-center gap-3 px-4 py-3 rounded-lg
                                                text-left font-medium transition-all duration-200
                                                ${isParentActive(item)
                                                    ? 'bg-gradient-primary text-white shadow-md'
                                                                    : 'text-gray-600 hover:bg-gray-50'
                                                }
                                            `}
                                        >
                                            <i className={`${item.icon} text-base`} />
                                            <span className="text-sm flex-1">{item.label}</span>
                                            <i className={`pi ${expandedSections[item.label] ? 'pi-chevron-down' : 'pi-chevron-right'} text-xs transition-transform`} />
                                        </button>

                                        {/* Sub Items */}
                                        {expandedSections[item.label] && (
                                            <ul className="mt-1 ml-4 space-y-1">
                                                {item.subItems.map((subItem, subIndex) => (
                                                    <li key={subIndex}>
                                                        <button
                                                            onClick={() => handleNavigation(subItem.path)}
                                                            className={`
                                                                w-full flex items-center gap-3 px-4 py-2.5 rounded-lg
                                                                text-left font-medium transition-all duration-200
                                                                ${isActive(subItem.path)
                                                                    ? 'bg-gradient-primary text-white shadow-md'
                                                                    : 'text-gray-600 hover:bg-gray-50'
                                                                }
                                                            `}
                                                        >
                                                            <div className={`w-1.5 h-1.5 rounded-full ${isActive(subItem.path) ? 'bg-white' : 'bg-gray-400'}`} />
                                                            <span className="text-sm">{subItem.label}</span>
                                                        </button>
                                                    </li>
                                                ))}
                                            </ul>
                                        )}
                                    </div>
                                ) : (
                                    /* Single Item without sub-menu */
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
                                        <i className={`${item.icon} text-base`} />
                                        <span className="text-sm">{item.label}</span>
                                    </button>
                                )}
                            </li>
                        ))}
                    </ul>
                </nav>
            </aside>
        </>
    );
};

export default Sidebar;
