import reactsvg from "@/assets/react.svg"
import { MdCheckCircle } from "react-icons/md"

export default function HowItWorks() {
  const steps = [
    {
      title: "Ingestion & Uploading",
      description: "Connect your data source to Pulse, or upload spreadsheet and cloud data.",
    },
    {
      title: "Schema Validation & Storage",
      description: "Schema is mapped to dimensional data model, stored in secure cloud.",
    },
    {
      title: "Processing & Transformation",
      description: "Data is processed and transformed to generate actionable insights.",
    },
    {
      title: "Generating Analytics",
      description: "The system creates dashboards, reports, and actionable analytics.",
    },
    {
      title: "Forecasting & Predictions",
      description: "Build models on your data, forecasts for business planning.",
    },
    {
      title: "Visualization",
      description: "Beautiful visualizations, dashboards, reports and exports.",
    },
  ]

  return (
    <section className="min-h-screen bg-white flex items-center justify-center py-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 w-full">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold text-gray-900 mb-2">How it works</h2>
          <p className="text-gray-600">Process</p>
          <p className="text-sm text-gray-500 mt-2">
            A robust pipeline process ingesting, storage, transformation,
            <br />
            analytics, ML, and visualization
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 items-center">
          {/* Left Image */}
          <div className="flex justify-center lg:justify-start order-2 lg:order-1">
            <div className="relative w-full h-96 max-w-sm">
              <img
                src={reactsvg}
                alt="Data Processing Team"
                className="object-contain"
              />
            </div>
          </div>

          {/* Right Timeline */}
          <div className="space-y-6 order-1 lg:order-2">
            {steps.map((step, index) => (
              <div key={index} className="flex gap-4">
                <div className="flex-shrink-0">
                  <div className="flex items-center justify-center h-12 w-12 rounded-full bg-primary">
                    <MdCheckCircle className="h-6 w-6 text-white" />
                  </div>
                </div>
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-900">{step.title}</h3>
                  <p className="text-gray-600 text-sm mt-1">{step.description}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-12 text-center text-gray-600 text-sm">
          <p>All this processing is done using parallel and distributed computing and processing.</p>
        </div>

        {/* Right Illustration */}
        <div className="flex justify-center mt-12">
          <div className="relative w-full h-96 max-w-md">
            <img src={reactsvg} alt="Analytics Team" className="object-contain" />
          </div>
        </div>
      </div>
    </section>
  )
}
