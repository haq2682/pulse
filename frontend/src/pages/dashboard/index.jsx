import React, { useState, useRef, useEffect } from 'react';
import Sidebar from './Sidebar';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import { InputText } from 'primereact/inputtext';
import { Button } from 'primereact/button';
import { Dropdown } from 'primereact/dropdown';
import { useAuth } from '@/context/AuthContext';
import axiosInstance from '@/services/api/axiosInstance';
import { useNavigate, useParams } from 'react-router-dom';

const Dashboard = () => {
    const { logout, user } = useAuth();
    const [sidebarOpen, setSidebarOpen] = useState(false);
    const [selectedItem, setSelectedItem] = useState(null);
    const [selectedBusiness, setSelectedBusiness] = useState(null);
    const navigate = useNavigate();
    const [isAddBusinessLoading, setIsAddBusinessLoading] = useState(false);

    const { businessId } = useParams();
    // NEW: State to toggle the custom profile menu
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    // NEW: Ref to detect clicks outside the menu to close it
    const profileRef = useRef(null);

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
                navigate(`/analytics/${businessList[0].business_id}`);
            }

        } catch (error) {
            console.error("Error fetching businesses:", error);
        }
    }

    const handleBusinessChange = (e) => {
        setSelectedBusiness(e.value);
        navigate(`/analytics/${e.value}`);
    }

    useEffect(() => {
        getBusinesses();
    }, []);

    useEffect(() => {
        if (businessId && businesses.length > 0) {
            // Only set if different
            if (selectedBusiness !== businessId) {
                setSelectedBusiness(businessId);
            }
        }
    }, [businessId, businesses]);

    return (
        <div className="flex h-screen overflow-hidden bg-gray-50">
            {/* Sidebar */}
            <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

            {/* Main Content */}
            <div className="flex-1 flex flex-col overflow-hidden">
                {/* Top Header */}
                <header className="bg-white border-b border-gray-200 px-4 md:px-6 py-4 flex items-center justify-between">
                    <button
                        onClick={() => setSidebarOpen(true)}
                        className="lg:hidden p-2 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        <i className="pi pi-bars text-xl text-gray-700"></i>
                    </button>

                    <Heading level={3} gradient={true} className="hidden md:block text-xl md:text-2xl m-0">
                        Analytics Overview
                    </Heading>

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
                        <div className="mx-10">
                            <Button
                                onClick={handleAddBusiness}
                                className="bg-white text-gray-700 border border-gray-300 hover:border-[var(--color-g2)] hover:bg-gray-50 transition-all p-2"
                                style={{
                                    background: 'white',
                                    boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)'
                                }}
                                disabled={isAddBusinessLoading}
                            >
                                <div style={{ opacity: isAddBusinessLoading ? 0.5 : 1 }} className="flex items-center">
                                    <i className="pi pi-building mr-2"></i>
                                    <span className="font-medium text-xs sm:text-sm">Add Business/Organization</span>
                                </div>
                            </Button>
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
                    
                    <div className="flex items-center justify-center min-h-[60vh]">
                        <div className="text-center max-w-md">
                            <Text className="text-gray-500 text-base md:text-lg leading-relaxed">
                                You have not added any business yet. Please click on the "Add Business Button" above to add a business.
                            </Text>
                        </div>
                    </div>
                </main>
            </div>
        </div>
    );
};

export default Dashboard;