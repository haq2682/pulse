import React from 'react';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';

const ExportAnalytics = () => {
    return (
        <div className="p-6 space-y-6">
            <div>
                <Heading level={2} className="text-2xl font-bold text-gray-800">
                    📤 Export Analytics
                </Heading>
                <Text className="text-gray-500 mt-1">
                    Export your analytics reports and schedule automated data deliveries.
                </Text>
            </div>

            <div className="flex items-center justify-center min-h-[50vh]">
                <div className="text-center max-w-md space-y-4">
                    <div className="text-6xl">📤</div>
                    <Heading level={3} className="text-xl font-semibold text-gray-700">
                        Coming Soon
                    </Heading>
                    <Text className="text-gray-500">
                        Export Analytics will let you download reports in multiple formats,
                        schedule recurring exports, and review export history — all from
                        one place.
                    </Text>
                </div>
            </div>
        </div>
    );
};

export default ExportAnalytics;
