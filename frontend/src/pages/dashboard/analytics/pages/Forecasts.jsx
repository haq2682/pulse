import React from 'react';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';

const Forecasts = () => {
    return (
        <div className="p-6 space-y-6">
            <div>
                <Heading level={2} className="text-2xl font-bold text-gray-800">
                    🔮 Forecasts &amp; Predictions
                </Heading>
                <Text className="text-gray-500 mt-1">
                    AI-powered forecasts and predictive analytics for your business.
                </Text>
            </div>

            <div className="flex items-center justify-center min-h-[50vh]">
                <div className="text-center max-w-md space-y-4">
                    <div className="text-6xl">🔮</div>
                    <Heading level={3} className="text-xl font-semibold text-gray-700">
                        Coming Soon
                    </Heading>
                    <Text className="text-gray-500">
                        Forecasts &amp; Predictions will provide revenue forecasting, demand planning,
                        and churn prediction capabilities powered by machine learning.
                    </Text>
                </div>
            </div>
        </div>
    );
};

export default Forecasts;
