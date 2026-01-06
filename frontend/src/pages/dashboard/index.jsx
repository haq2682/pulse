import React, { useState } from 'react';
import Sidebar from './Sidebar';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import { InputText } from 'primereact/inputtext';
import { Button } from 'primereact/button';
import { Dropdown } from 'primereact/dropdown';

const Dashboard = () => {
    const [sidebarOpen, setSidebarOpen] = useState(false);
    const [selectedItem, setSelectedItem] = useState(null);
    const items = Array.from({ length: 100000 }).map((_, i) => ({ label: `Item #${i}`, value: i }));

    const handleAddBusiness = () => {
        // TODO: Implement add business modal/page
        console.log('Add business clicked');
    };

    return (
        <div className="flex h-screen overflow-hidden bg-gray-50">
            {/* Sidebar */}
            <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

            {/* Main Content */}
            <div className="flex-1 flex flex-col overflow-hidden">
                {/* Top Header */}
                <header className="bg-white border-b border-gray-200 px-4 md:px-6 py-4 flex items-center justify-between">
                    {/* Mobile Menu Button */}
                    <button
                        onClick={() => setSidebarOpen(true)}
                        className="lg:hidden p-2 hover:bg-gray-100 rounded-lg transition-colors"
                    >
                        <i className="pi pi-bars text-xl text-gray-700"></i>
                    </button>

                    {/* Page Title */}
                    <Heading level={3} gradient={true} className="hidden md:block text-xl md:text-2xl m-0">
                        Analytics Overview
                    </Heading>

                    <InputText type="text" className="p-inputtext-sm w-2/4" placeholder="Search Insight..." />

                    {/* Right Side - Notifications & Avatar */}
                    <div className="flex items-center gap-3">
                        <button className="p-2 hover:bg-gray-100 rounded-full transition-colors relative">
                            <i className="pi pi-bell text-xl text-gray-700"></i>
                        </button>
                        <div className="w-10 h-10 rounded-full bg-gradient-primary flex items-center justify-center text-white font-bold cursor-pointer hover:opacity-90 transition-opacity">
                            <i className="pi pi-user text-lg"></i>
                        </div>
                    </div>
                </header>

                {/* Main Content Area */}
                <main className="flex-1 overflow-y-auto p-4 md:p-6">
                    <div className="flex items-center justify-between">
                        {/* Add Business Button */}
                        <div classname="mx-10">
                            <Button
                                onClick={handleAddBusiness}
                                className="bg-white text-gray-700 border border-gray-300 hover:border-[var(--color-g2)] hover:bg-gray-50 transition-all p-2"
                                style={{
                                    background: 'white',
                                    boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)'
                                }}
                            >
                                <i className="pi pi-building mr-2"></i>
                                <span className="font-medium text-xs sm:text-sm">Add Business/Organization</span>
                            </Button>
                        </div>
                        <div className="w-48">
                            <Dropdown value={selectedItem} onChange={(e) => setSelectedItem(e.value)} options={items} virtualScrollerOptions={{ itemSize: 38 }}
                                placeholder="Select Business" className="w-full" />
                        </div>
                    </div>


                    {/* Empty State */}
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