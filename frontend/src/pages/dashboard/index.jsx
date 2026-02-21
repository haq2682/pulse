import React, { useState, useRef, useEffect } from 'react';
import Sidebar from './Sidebar';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import { InputText } from 'primereact/inputtext';
import { Button } from 'primereact/button';
import { Dropdown } from 'primereact/dropdown';
import { Dialog } from 'primereact/dialog';
import { Toast } from 'primereact/toast';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import { useAuth } from '@/context/AuthContext';
import { usePipelineProgress } from '@/context/PipelineProgressContext';
import axiosInstance from '@/services/api/axiosInstance';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import InlinePipelineProgress from '@/components/global/InlinePipelineProgress';
import ExecutiveOverview from './analytics/pages/ExecutiveOverview';
import CustomerOverview from './analytics/pages/CustomerOverview';
import CustomerSegmentation from './analytics/pages/CustomerSegmentation';
import CustomerHealthRetention from './analytics/pages/CustomerHealthRetention';
import CustomerValueAnalysis from './analytics/pages/CustomerValueAnalysis';
import ProductPerformance from './analytics/pages/ProductPerformance';
import ProductProfitability from './analytics/pages/ProductProfitability';
import ProductEngagement from './analytics/pages/ProductEngagement';
import ProductTrends from './analytics/pages/ProductTrends';
import InventoryHealth from './analytics/pages/InventoryHealth';
import InventoryReorderManagement from './analytics/pages/InventoryReorderManagement';
import InventoryEfficiency from './analytics/pages/InventoryEfficiency';
import InventorySupplier from './analytics/pages/InventorySupplier';
import SupplierPerformance from './analytics/pages/SupplierPerformance';
import SupplierOperations from './analytics/pages/SupplierOperations';
import SupplierEconomics from './analytics/pages/SupplierEconomics';
import MarketingCampaigns from './analytics/pages/MarketingCampaigns';
import MarketingAttribution from './analytics/pages/MarketingAttribution';
import MarketingChannels from './analytics/pages/MarketingChannels';
import FunnelOverview from './analytics/pages/FunnelOverview';
import FunnelCart from './analytics/pages/FunnelCart';
import FunnelCheckout from './analytics/pages/FunnelCheckout';
import FunnelWishlist from './analytics/pages/FunnelWishlist';
import PaymentMethods from './analytics/pages/PaymentMethods';
import PaymentRefunds from './analytics/pages/PaymentRefunds';
import PaymentFinancialMetrics from './analytics/pages/PaymentFinancialMetrics';
import OperationsProcessing from './analytics/pages/OperationsProcessing';
import OperationsDelivery from './analytics/pages/OperationsDelivery';
import OperationsShipping from './analytics/pages/OperationsShipping';
import RecommendationsProductAffinity from './analytics/pages/RecommendationsProductAffinity';
import RecommendationsCategoryAffinity from './analytics/pages/RecommendationsCategoryAffinity';
import RecommendationsCoverage from './analytics/pages/RecommendationsCoverage';
import ReviewsOverview from './analytics/pages/ReviewsOverview';
import ReviewsSentiment from './analytics/pages/ReviewsSentiment';
import ReviewsImpact from './analytics/pages/ReviewsImpact';
import EngagementMetrics from './analytics/pages/EngagementMetrics';
import EngagementBehavior from './analytics/pages/EngagementBehavior';
import EngagementConversion from './analytics/pages/EngagementConversion';

const Dashboard = () => {
    const { logout, user } = useAuth();
    const { startPipeline } = usePipelineProgress();
    const [sidebarOpen, setSidebarOpen] = useState(false);
    const [selectedBusiness, setSelectedBusiness] = useState(null);
    const navigate = useNavigate();
    const [isAddBusinessLoading, setIsAddBusinessLoading] = useState(false);
    const [isDeleteBusinessLoading, setIsDeleteBusinessLoading] = useState(false);
    const [businessIngestionType, setBusinessIngestionType] = useState(null);
    const [showDeleteDialog, setShowDeleteDialog] = useState(false);

    const { businessId } = useParams();
    // NEW: State to toggle the custom profile menu
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    // NEW: Ref to detect clicks outside the menu to close it
    const profileRef = useRef(null);
    // NEW: Pipeline status for streaming modes
    const [pipelineStatus, setPipelineStatus] = useState('idle');
    // NEW: Toast ref for notifications
    const toastRef = useRef(null);

    // Mock data
    const [businesses, setBusinesses] = useState([]);

    // Close menu when clicking outside
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (profileRef.current && !profileRef.current.contains(event.target)) {
                setIsProfileOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const handleAddBusiness = async () => {
        setIsAddBusinessLoading(true);
        const response = await axiosInstance.post('/onboarding/create', {userId: user.user_id});
        let current_step = response.data.current_step;
        if(current_step === 'mapping-in-progress') {
            current_step = 'connect';
        }
        if(response.data.status === 200) {
            navigate(`/onboarding/${current_step}/${response.data.onboarding_id}`);
        }
        setIsAddBusinessLoading(false);
    };

    const getBusinesses = async () => {
        try {
            const response = await axiosInstance.get('/analytics/get-businesses', {
                params: { userId: user.user_id }
            });
            const businessList = response.data.businesses || [];
            setBusinesses(businessList);

            // Redirect to first business if URL has no business ID
            if (!businessId && businessList.length > 0) {
                setBusinessIngestionType(businessList[0].ingestion_type);
                navigate(`/analytics/${businessList[0].business_id}`);
            }

        } catch (error) {
            console.error("Error fetching businesses:", error);
        }
    }

    const handleBusinessChange = (e) => {
        setSelectedBusiness(e.value);
        setBusinessIngestionType(e.option?.ingestion_type || null);
        navigate(`/analytics/${e.value}`);
    }

    useEffect(() => {
        getBusinesses();
    }, []);

    useEffect(() => {
        if (businessId && businesses.length > 0) {
            // Only set if different
            if (selectedBusiness !== businessId) {
                const business = businesses.find(b => b.business_id === businessId);
                if (business) {
                    setBusinessIngestionType(business.ingestion_type);
                }
                setSelectedBusiness(businessId);
            }
        }
    }, [businessId, businesses]);
    
    // Handle starting analysis
    const handleStartAnalysis = async () => {
        if (!businessId || !user?.user_id) {
            console.error('Missing businessId or user_id');
            return;
        }
        
        try {
            const result = await startPipeline(businessId);
            if (!result.success) {
                console.error('Failed to start pipeline:', result.error);
            }
        } catch (err) {
            console.error('Error starting pipeline:', err);
        }
    };
    
    // Handle delete business with confirmation
    const handleDeleteBusiness = () => {
        if (!selectedBusiness) {
            return;
        }
        setShowDeleteDialog(true);
    };
    
    const performDeleteBusiness = async () => {
        if (!selectedBusiness || !user?.user_id) {
            return;
        }
        
        setIsDeleteBusinessLoading(true);
        
        try {
            const response = await axiosInstance.delete('/analytics/delete-business', {
                data: {
                    userId: user.user_id,
                    businessId: selectedBusiness
                }
            });
            
            if (response.data.status === 200) {
                console.log('Business deleted successfully');
                setShowDeleteDialog(false);
                // Redirect to analytics page without business ID
                navigate('/analytics/');
                // Refresh business list
                await getBusinesses();
            }
        } catch (error) {
            console.error('Error deleting business:', error);
            alert('Failed to delete business. Please try again.');
        } finally {
            setIsDeleteBusinessLoading(false);
        }
    };

    // Function to trigger streaming pipeline
    const triggerStreamingPipeline = async () => {
        setPipelineStatus('running');
        
        try {
            const response = await axiosInstance.post('/pipeline/trigger-streaming', {
                businessId: businessId
            });
            
            if (response.data.success) {
                setPipelineStatus('success');
                toastRef.current?.show({
                    severity: 'success',
                    summary: 'Success',
                    detail: 'Streaming pipeline triggered successfully',
                    life: 3000
                });
                // Return to idle after 3 seconds
                setTimeout(() => setPipelineStatus('idle'), 3000);
            } else {
                throw new Error('Pipeline trigger failed');
            }
        } catch (error) {
            setPipelineStatus('failed');
            toastRef.current?.show({
                severity: 'error',
                summary: 'Error',
                detail: 'Failed to trigger streaming pipeline',
                life: 5000
            });
            // Return to idle after 5 seconds
            setTimeout(() => setPipelineStatus('idle'), 5000);
        }
    };

    // Ingestion Status Indicator Component
    const IngestionStatusIndicator = ({
        ingestionType,
        pipelineStatus,
        onTriggerPipeline
    }) => {

        const getStatusConfig = () => {
            if (ingestionType === 'batch') {
            return {
                borderColor: 'border-purple-500',
                dotColor: 'bg-purple-500',
                glow: 'shadow-[0_0_5px_2px_rgba(168,85,247,0.7)]',
                text: 'Batch',
                showRefresh: false,
                rotating: false,
                disabled: false,
                pulse: true
            };
            }

            const text = ingestionType === 'api' ? 'API' : 'Database';

            if (pipelineStatus === 'running') {
            return {
                borderColor: 'border-yellow-500',
                dotColor: 'bg-yellow-500',
                glow: 'shadow-[0_0_10px_3px_rgba(234,179,8,0.9)]',
                text,
                showRefresh: true,
                rotating: true,
                disabled: true,
                pulse: false
            };
            }

            if (pipelineStatus === 'failed') {
            return {
                borderColor: 'border-red-500',
                dotColor: 'bg-red-500',
                glow: 'shadow-[0_0_8px_2px_rgba(239,68,68,0.8)]',
                text,
                showRefresh: true,
                rotating: false,
                disabled: false,
                pulse: true
            };
            }

            return {
            borderColor: 'border-green-500',
            dotColor: 'bg-green-500',
            glow: 'shadow-[0_0_8px_2px_rgba(34,197,94,0.8)]',
            text,
            showRefresh: true,
            rotating: false,
            disabled: false,
            pulse: true
            };
        };

        const config = getStatusConfig();

        return (
            <div
            className={`
                border-2 ${config.borderColor}
                rounded-lg px-3 py-2 
                flex items-center gap-2
                transition-all duration-300
            `}
            >
            {/* Glowing Status Dot */}
            <div
                className={`
                w-2.5 h-2.5 rounded-full
                ${config.dotColor}
                ${config.glow}
                ${config.pulse ? 'animate-pulse' : ''}
                transition-all duration-300
                `}
            />

            <span className="text-sm font-medium text-gray-700">
                {config.text}
            </span>

            {config.showRefresh && (
                <button
                onClick={onTriggerPipeline}
                disabled={config.disabled}
                className={`
                    ml-1 p-1 hover:bg-gray-100 rounded
                    transition-colors duration-200
                    ${config.rotating ? 'animate-spin' : ''}
                    ${config.disabled ? 'opacity-50 cursor-not-allowed' : ''}
                `}
                title="Trigger streaming pipeline"
                >
                <i className="pi pi-refresh text-sm text-gray-600" />
                </button>
            )}
            </div>
        );
    };

    const businessName = businesses.find(b => b.business_id === selectedBusiness)?.business_name || 'this business';

    const deleteDialogFooter = (
        <div className="flex justify-end gap-2 mb-5 mr-5">
            <SecondaryButton 
                onClick={() => setShowDeleteDialog(false)}
                disabled={isDeleteBusinessLoading}
                label="Cancel"
                success
            >
            </SecondaryButton>
            <PrimaryButton 
                onClick={performDeleteBusiness}
                loading={isDeleteBusinessLoading}
                label={isDeleteBusinessLoading ? 'Deleting...' : 'Delete'}
                danger
            />
        </div>
    );

    // Get current location for route-based rendering
    const location = useLocation();
    const pathname = location.pathname;

    // Render appropriate analytics content based on route
    const renderAnalyticsContent = () => {
        if (!businessId) return null;

        // Executive Overview - exact match
        if (pathname === `/analytics/${businessId}` || pathname === `/analytics/${businessId}/`) {
            return <ExecutiveOverview />;
        }

        // Customers routes
        if (pathname.includes('/customers/overview')) {
            return <CustomerOverview />;
        }

        if (pathname.includes('/customers/segmentation')) {
            return <CustomerSegmentation />;
        }

        if (pathname.includes('/customers/health')) {
            return <CustomerHealthRetention />;
        }

        if (pathname.includes('/customers/value')) {
            return <CustomerValueAnalysis />;
        }

        // Products routes
        if (pathname.includes('/products/performance')) {
            return <ProductPerformance />;
        }

        if (pathname.includes('/products/profitability')) {
            return <ProductProfitability />;
        }

        if (pathname.includes('/products/engagement')) {
            return <ProductEngagement />;
        }

        if (pathname.includes('/products/trends')) {
            return <ProductTrends />;
        }

        // Inventory routes
        if (pathname.includes('/inventory/health')) {
            return <InventoryHealth />;
        }

        if (pathname.includes('/inventory/reorder')) {
            return <InventoryReorderManagement />;
        }

        if (pathname.includes('/inventory/efficiency')) {
            return <InventoryEfficiency />;
        }

        if (pathname.includes('/inventory/supplier')) {
            return <InventorySupplier />;
        }

        // Supplier routes
        if (pathname.includes('/suppliers/performance')) {
            return <SupplierPerformance />;
        }

        if (pathname.includes('/suppliers/operations')) {
            return <SupplierOperations />;
        }

        if (pathname.includes('/suppliers/economics')) {
            return <SupplierEconomics />;
        }

        // Marketing routes
        if (pathname.includes('/marketing/campaigns')) {
            return <MarketingCampaigns />;
        }

        if (pathname.includes('/marketing/attribution')) {
            return <MarketingAttribution />;
        }

        if (pathname.includes('/marketing/channels')) {
            return <MarketingChannels />;
        }

        // Funnel routes
        if (pathname.includes('/funnel/overview')) {
            return <FunnelOverview />;
        }

        if (pathname.includes('/funnel/cart')) {
            return <FunnelCart />;
        }

        if (pathname.includes('/funnel/checkout')) {
            return <FunnelCheckout />;
        }

        if (pathname.includes('/funnel/wishlist')) {
            return <FunnelWishlist />;
        }

        // Payments & Finance routes
        if (pathname.includes('/payments/methods')) {
            return <PaymentMethods />;
        }

        if (pathname.includes('/payments/refunds')) {
            return <PaymentRefunds />;
        }

        if (pathname.includes('/payments/metrics')) {
            return <PaymentFinancialMetrics />;
        }

        // Operations routes
        if (pathname.includes('/operations/processing')) {
            return <OperationsProcessing />;
        }

        if (pathname.includes('/operations/delivery')) {
            return <OperationsDelivery />;
        }

        if (pathname.includes('/operations/shipping')) {
            return <OperationsShipping />;
        }

        // Recommendations routes
        if (pathname.includes('/recommendations/product')) {
            return <RecommendationsProductAffinity />;
        }

        if (pathname.includes('/recommendations/category')) {
            return <RecommendationsCategoryAffinity />;
        }

        if (pathname.includes('/recommendations/coverage')) {
            return <RecommendationsCoverage />;
        }

        // Reviews & Sentiment routes
        if (pathname.includes('/reviews/overview')) {
            return <ReviewsOverview />;
        }

        if (pathname.includes('/reviews/sentiment')) {
            return <ReviewsSentiment />;
        }

        if (pathname.includes('/reviews/impact')) {
            return <ReviewsImpact />;
        }

        // Engagement routes
        if (pathname.includes('/engagement/metrics')) {
            return <EngagementMetrics />;
        }

        if (pathname.includes('/engagement/behavior')) {
            return <EngagementBehavior />;
        }

        if (pathname.includes('/engagement/conversion')) {
            return <EngagementConversion />;
        }

        // Default fallback for unrecognized routes
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <div className="text-center max-w-md">
                    <p className="text-gray-500 text-lg">
                        Page not fond. Please select a valid analytics page from the sidebar.
                    </p>
                </div>
            </div>
        );
    };

    return (
        <div className="flex h-screen overflow-hidden bg-gray-50">
            {/* Toast for notifications */}
            <Toast ref={toastRef} />
            
            {/* Delete Business Dialog */}
            <Dialog
                visible={showDeleteDialog}
                onHide={() => setShowDeleteDialog(false)}
                header="Delete Business"
                footer={deleteDialogFooter}
                style={{ width: '450px' }}
                modal
            >
                <div>
                    <p>Are you sure you want to delete <strong>{businessName}</strong>?</p>
                    <p className="text-red-600 text-sm mt-2">
                        This will permanently delete:
                    </p>
                    <ul className="text-sm text-red-600 list-disc list-inside mt-1">
                        <li>All pipeline data</li>
                        <li>All processed data from storage</li>
                        <li>All business records</li>
                    </ul>
                    <p className="text-sm text-gray-600 mt-2">
                        This action cannot be undone.
                    </p>
                </div>
            </Dialog>
            
            {/* Sidebar */}
            <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

            {/* Main Content */}
            <div className="flex-1 flex flex-col overflow-hidden">
                {/* Top Header */}
                <header className="bg-white border-b border-gray-200 px-4 md:px-6 py-4 flex items-center justify-between gap-4">
                    <button
                        onClick={() => setSidebarOpen(true)}
                        className="lg:hidden p-2 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        <i className="pi pi-bars text-xl text-gray-700"></i>
                    </button>

                    <div className="flex items-center gap-4">
                        <Heading level={3} gradient={true} className="hidden md:block text-xl md:text-2xl m-0">
                            Dashboard
                        </Heading>
                        {/* Ingestion Status Indicator */}
                        {businessId && businessIngestionType && (
                            <IngestionStatusIndicator 
                                ingestionType={businessIngestionType}
                                pipelineStatus={pipelineStatus}
                                onTriggerPipeline={triggerStreamingPipeline}
                            />
                        )}
                    </div>
                    <InputText type="text" className="p-inputtext-sm w-2/4" placeholder="Search Insight..." />

                    {/* Right Side - Notifications & Avatar */}
                    <div className="flex items-center gap-3">
                        <button className="p-2 hover:bg-gray-100 rounded-full transition-colors relative">
                            <i className="pi pi-bell text-xl text-gray-700"></i>
                        </button>
                        
                        {/* PROFILE DROPDOWN CONTAINER */}
                        <div className="relative" ref={profileRef}>
                            {/* Avatar Trigger */}
                            <div 
                                onClick={() => setIsProfileOpen(!isProfileOpen)}
                                className={`
                                    w-10 h-10 rounded-full bg-gradient-primary 
                                    flex items-center justify-center text-white font-bold 
                                    cursor-pointer hover:opacity-90 transition-all shadow-sm
                                    ${isProfileOpen ? 'ring-2 ring-offset-1 ring-[var(--color-primary)]' : ''}
                                `}
                            >
                                {user?.username ? (
                                    <span className="uppercase">{user.username.charAt(0)}</span>
                                ) : (
                                    <i className="pi pi-user text-lg"></i>
                                )}
                            </div>

                            {/* CUSTOM MENU DROPDOWN */}
                            {isProfileOpen && (
                                <div className="absolute right-0 mt-3 w-48 bg-white rounded-xl shadow-xl border border-gray-100 py-2 z-50 animate-fade-in-down origin-top-right">
                                    {/* User Info (Optional Header) */}
                                    <div className="px-4 py-2 border-b border-gray-100 mb-1">
                                        <p className="text-sm font-semibold text-gray-800 truncate">{user?.username || 'User'}</p>
                                        <p className="text-xs text-gray-500 truncate">{user?.email}</p>
                                    </div>

                                    {/* Menu Items */}
                                    <button 
                                        onClick={() => console.log('Profile')}
                                        className="w-full text-left px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                    >
                                        <i className="pi pi-user text-gray-500"></i>
                                        Profile
                                    </button>

                                    <button 
                                        onClick={logout}
                                        className="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2 transition-colors"
                                    >
                                        <i className="pi pi-sign-out text-red-500"></i>
                                        Log Out
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>
                </header>

                {/* Main Content Area */}
                <main className="flex-1 overflow-y-auto p-4 md:p-6">
                    <div className="flex items-center justify-between">
                        <div className="mx-10 flex items-center gap-2">
                            {/* Add Business Button */}
                            <SecondaryButton
                                onClick={handleAddBusiness}
                                icon="pi pi-building"
                                disabled={isAddBusinessLoading}
                                label={isAddBusinessLoading ? 'Adding...' : 'Add Business'}
                                black
                            >
                            </SecondaryButton>
                            
                            {/* Delete Business Button (Icon Only) */}
                            {selectedBusiness && (
                                <Button
                                    onClick={handleDeleteBusiness}
                                    className="bg-white text-red-600 border border-red-300 hover:border-red-500 hover:bg-red-50 transition-all p-2"
                                    style={{
                                        background: 'white',
                                        boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)',
                                        width: '44px',
                                        height: '44px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center'
                                    }}
                                    disabled={isDeleteBusinessLoading}
                                    title="Delete Business"
                                >
                                    <i className={`pi ${isDeleteBusinessLoading ? 'pi-spin pi-spinner' : 'pi-trash'} text-lg`}></i>
                                </Button>
                            )}
                        </div>
                        <div className="w-48">
                            <Dropdown 
                                value={selectedBusiness} 
                                onChange={handleBusinessChange} 
                                options={businesses.map (b => ({ label: b.business_name, value: b.business_id }))}
                                virtualScrollerOptions={{ itemSize: 38 }}
                                placeholder="Select Business" 
                                className="w-full" 
                            />
                        </div>
                    </div>
                    
                    {/* Show inline pipeline progress when business is selected */}
                    {businessId ? (
                        <>
                            <InlinePipelineProgress 
                                businessId={businessId}
                                onStartAnalysis={handleStartAnalysis}
                            />
                            {/* Render appropriate analytics content based on route */}
                            {renderAnalyticsContent()}
                        </>
                    ) : (
                        <div className="flex items-center justify-center min-h-[60vh]">
                            <div className="text-center max-w-md">
                                <Text className="text-gray-500 text-base md:text-lg leading-relaxed">
                                    You have not added any business yet. Please click on the "Add Business Button" above to add a business.
                                </Text>
                            </div>
                        </div>
                    )}
                </main>
            </div>
        </div>
    );
};

export default Dashboard;