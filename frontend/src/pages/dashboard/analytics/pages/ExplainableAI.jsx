import React from 'react';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';

const ExplainableAI = () => {
    return (
        <div className="p-6 space-y-6">
            <div>
                <Heading level={2} className="text-2xl font-bold text-gray-800">
                    🤖 Explainable AI
                </Heading>
                <Text className="text-gray-500 mt-1">
                    Understand the reasoning behind AI-driven insights and recommendations.
                </Text>
            </div>

            <div className="flex items-center justify-center min-h-[50vh]">
                <div className="text-center max-w-md space-y-4">
                    <div className="text-6xl">🤖</div>
                    <Heading level={3} className="text-xl font-semibold text-gray-700">
                        Coming Soon
                    </Heading>
                    <Text className="text-gray-500">
                        Explainable AI will surface feature importance scores, model decision
                        explanations, and interpretable insights to help you understand
                        the drivers behind every recommendation.
                    </Text>
                </div>
            </div>
        </div>
    );
};

export default ExplainableAI;
