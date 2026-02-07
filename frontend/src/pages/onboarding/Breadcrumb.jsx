import React from 'react';
import { useState } from 'react';
import { SecondaryButton } from '../../components/global/Button';
import axiosInstance from '@/services/api/axiosInstance';
import { useNavigate } from 'react-router';
import { useAuth } from '@/context/AuthContext';

const Breadcrumb = ({ items }) => {
    const navigate = useNavigate();
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);
    const { user } = useAuth();
    const handleCancel = async () => {
        setLoading(true);
        try {
            const response = await axiosInstance.delete('/onboarding/cancel', { data: { userId: user.user_id } });
            if(response.status === 200) navigate('/analytics');
        }
        catch (err) {
            setError('An error occurred while cancelling the onboarding process. Please try again.');
        }
        finally {
            setLoading(false);
        }
    }
    return (
        <div className="w-full">
            <nav className="flex items-center gap-2 text-sm md:text-base flex-wrap">
                {items.map((item, index) => (
                    <React.Fragment key={index}>
                        <span
                            className={`
                                font-medium transition-colors
                                ${item.active
                                    ? 'text-[var(--color-g1)] font-semibold'
                                    : 'text-gray-500'
                                }
                                ${item.clickable ? 'cursor-pointer hover:text-[var(--color-g2)]' : ''}
                            `}
                            onClick={item.onClick}
                        >
                            {item.label}
                        </span>
                        {index < items.length - 1 && (
                            <span className="text-gray-400 font-medium">{'>'}</span>
                        )}
                    </React.Fragment>
                ))}
                <SecondaryButton label="Cancel" danger onClick={handleCancel} disabled={loading} loading={loading}/>
            </nav>
            {error && (
                <div className="p-3 bg-red-100 text-red-700 border border-red-400 rounded w-3/4 mt-4 text-center place-self-center">
                    {error}
                </div>
            )}
        </div>
    );
};

export default Breadcrumb;