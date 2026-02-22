import React, { useEffect, useState } from 'react';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
import { Card } from 'primereact/card';
import { Button } from 'primereact/button';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import { useAdminAuth } from '@/context/AdminAuthContext';
import adminApi from '@/services/api/adminApi';
import usePageTitle from '@/hooks/usePageTitle';

const AdminDashboard = () => {
    usePageTitle('Admin Dashboard');
    const { logout, admin } = useAdminAuth();
    const [stats, setStats] = useState({ total_users: 0, total_businesses: 0 });
    const [tableData, setTableData] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        try {
            const data = await adminApi.getDashboardStats();
            setStats(data.stats);
            setTableData(data.table_data);
        } catch (error) {
            console.error("Failed to fetch admin stats", error);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gray-50 flex flex-col">
            {/* Top Bar */}
            <header className="bg-white shadow-sm border-b border-gray-200 px-6 py-4 flex justify-between items-center">
                <Heading level={3} gradient={true} className="m-0 text-xl">Pulse Admin</Heading>
                <div className="flex items-center gap-4">
                    <span className="text-sm text-gray-600">Welcome, {admin?.username || 'Admin'}</span>
                    <Button label="Logout" icon="pi pi-sign-out" size="small" outlined severity="danger" onClick={logout} />
                </div>
            </header>

            <main className="flex-1 p-6 max-w-7xl mx-auto w-full">
                {/* Stats Cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                    <Card className="shadow-sm border-l-4 border-blue-500">
                        <Text className="text-gray-500 font-medium ml-2 mt-1">Total Registered Users</Text>
                        <Heading level={2} className="text-4xl text-blue-600 mt-2 m-3">{stats.total_users}</Heading>
                    </Card>
                    <Card className="shadow-sm border-l-4 border-purple-500">
                        <Text className="text-gray-500 font-medium ml-2 mt-1">Total Active Businesses</Text>
                        <Heading level={2} className="text-4xl text-purple-600 mt-2 m-3">{stats.total_businesses}</Heading>
                    </Card>
                </div>

                {/* Users Table */}
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                    <div className="flex justify-between items-center mb-6">
                        <Heading level={4} className="m-0">System Users & Businesses</Heading>
                        <Button icon="pi pi-refresh" rounded text severity="secondary" onClick={fetchData} loading={loading} />
                    </div>

                    <DataTable value={tableData} paginator rows={10} loading={loading} stripedRows className="text-sm">
                        <Column field="username" header="User Name" sortable body={(rowData) => (
                            <div className="font-semibold text-gray-700">{rowData.username}</div>
                        )}></Column>
                        <Column field="email" header="Email Address" sortable></Column>
                        <Column field="business_name" header="Business Name" body={(rowData) => (
                            rowData.business_name ? 
                            <span className="px-2 py-1 bg-green-50 text-green-700 rounded-md text-xs font-bold">{rowData.business_name}</span> : 
                            <span className="text-gray-400 italic">No Business</span>
                        )}></Column>
                        <Column field="business_region" header="Region"></Column>
                    </DataTable>
                </div>
            </main>
        </div>
    );
};

export default AdminDashboard;