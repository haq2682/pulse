import { MdCheckCircle } from "react-icons/md"
import reactsvg from "@/assets/react.svg"

export default function BenefitsSection() {
    const benefits = [
        {
            title: "Competitive Advantage",
            description: "Stay ahead in the market with data-driven strategies.",
        },
        {
            title: "Optimized Operations",
            description: "Improve inventory management, pricing strategies, and marketing campaigns.",
        },
        {
            title: "Cost Savings",
            description: "Automate analysis and reduce manual effort.",
        },
        {
            title: "Informed Decisions",
            description: "Make accurate and effective data-driven decisions.",
        },
        {
            title: "Scalable & Robust",
            description: "Designed to handle growing data volumes and ensure continuous operation.",
        },
    ]

    return (
        <section className="min-h-screen bg-gray-50 flex items-center justify-center py-12">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 w-full">
                <div className="text-center mb-12">
                    <h2 className="text-3xl sm:text-4xl font-bold text-gray-900 mb-2">Why Pulse Analytics</h2>
                    <p className="text-gray-600">Benefits</p>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 items-center">
                    {/* Left Benefits */}
                    <div className="space-y-6">
                        {benefits.map((benefit, index) => (
                            <div key={index} className="flex gap-4">
                                <MdCheckCircle className="text-primary text-2xl flex-shrink-0 mt-1" />
                                <div>
                                    <h3 className="text-lg font-semibold text-gray-900">{benefit.title}</h3>
                                    <p className="text-gray-600 text-sm mt-1">{benefit.description}</p>
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Right Image */}
                    <div className="flex justify-center lg:justify-end">
                        <div className="relative w-full h-96 max-w-sm">
                            <img
                                src={reactsvg}
                                alt="Benefits Illustration"
                                className="object-contain"
                            />
                        </div>
                    </div>
                </div>
            </div>
        </section>
    )
}
