import { MdCheckCircle } from "react-icons/md"
import reactsvg from "@/assets/react.svg"

export default function DashboardPreview() {
    return (
        <section className="min-h-screen bg-linear-to-br from-primary via-primary-light to-primary-lighter flex items-center justify-center py-12 relative overflow-hidden">
            {/* Background decoration */}
            <div className="absolute inset-0 opacity-10">
                <svg className="w-full h-full" viewBox="0 0 1200 600" preserveAspectRatio="none">
                    <defs>
                        <pattern id="wave" x="0" y="0" width="100" height="100" patternUnits="userSpaceOnUse">
                            <path d="M0,50 Q25,25 50,50 T100,50" fill="none" stroke="white" strokeWidth="2" />
                        </pattern>
                    </defs>
                    <rect width="1200" height="600" fill="url(#wave)" />
                </svg>
            </div>

            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 w-full relative z-10">
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 items-center">
                    {/* Left Content */}
                    <div className="text-white space-y-6">
                        <h2 className="text-3xl sm:text-4xl font-bold leading-tight">See your business at a glance</h2>
                        <p className="text-white/90 leading-relaxed">
                            With Pulse Analytics, you can monitor your business performance in real time with beautiful dashboards,
                            custom reports, and timely alerts.
                        </p>
                        <div className="space-y-3 pt-4">
                            <div className="flex items-center gap-3">
                                <MdCheckCircle className="text-accent text-xl flex-shrink-0" />
                                <span className="text-white/90">Real-time data monitoring, analysis, and alerts</span>
                            </div>
                            <div className="flex items-center gap-3">
                                <MdCheckCircle className="text-accent text-xl flex-shrink-0" />
                                <span className="text-white/90">Customizable dashboards and reports</span>
                            </div>
                        </div>
                        <div className="flex flex-col sm:flex-row gap-4 pt-6">
                            <button className="bg-accent text-white px-8 py-3 rounded-lg hover:bg-accent/90 transition font-semibold">
                                Get Started
                            </button>
                            <button className="border-2 border-white text-white px-8 py-3 rounded-lg hover:bg-white/10 transition font-semibold">
                                Explore Features
                            </button>
                        </div>
                    </div>

                    {/* Right Image */}
                    <div className="flex justify-center lg:justify-end">
                        <div className="relative w-full h-96 max-w-md lg:max-w-full bg-white/10 rounded-lg p-4 backdrop-blur-sm">
                            <img
                                src={reactsvg}
                                alt="Dashboard Preview"
                                className="object-contain p-4"
                            />
                        </div>
                    </div>
                </div>
            </div>
        </section>
    )
}
