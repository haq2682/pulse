import { MdCheckCircle } from "react-icons/md"
import { FiTrendingUp, FiBarChart2, FiUsers, FiFilter, FiTarget, FiMapPin, FiDollarSign } from "react-icons/fi"

export default function FeaturesGrid() {
  const features = [
    {
      icon: <MdCheckCircle />,
      title: "Revenue & Growth Trends",
      description: "Track revenue patterns and growth",
    },
    {
      icon: <FiBarChart2 />,
      title: "Product & Category Performance",
      description: "Deep dive on assort and category perf",
    },
    {
      icon: <FiTrendingUp />,
      title: "Demand & Sales Forecasting",
      description: "Predict demand with confidence",
    },
    {
      icon: <FiUsers />,
      title: "Customer Segmentation (RFM)",
      description: "Segment, Loyalty & Plan Fit",
    },
    {
      icon: <FiFilter />,
      title: "Cohort Retention",
      description: "Understand trends and why",
    },
    {
      icon: <FiTarget />,
      title: "Geographic Performance",
      description: "Track sales pattern by geographic",
    },
    {
      icon: <FiMapPin />,
      title: "Inventory Optimization",
      description: "Optimize inventory, prevent shortages",
    },
    {
      icon: <FiDollarSign />,
      title: "ROI & AOV",
      description: "Track channel ROI, revenue to grow profitably",
    },
  ]

  return (
    <section className="min-h-screen bg-gray-50 flex items-center justify-center py-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 w-full">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold text-gray-900 mb-2">What you get with Pulse Analytics</h2>
          <p className="text-gray-600">Key Features</p>
          <p className="text-sm text-gray-500 mt-2">
            A complete platform and lots of benefits to <br />
            close, scale, and maintain your book.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
          {features.map((feature, index) => (
            <div key={index} className="border-2 border-primary rounded-lg p-6 hover:shadow-lg transition bg-white">
              <div className="text-primary text-2xl mb-4">{feature.icon}</div>
              <h3 className="text-lg font-bold text-gray-900 mb-2">{feature.title}</h3>
              <p className="text-gray-600 text-sm">{feature.description}</p>
            </div>
          ))}
        </div>

        <div className="bg-white rounded-lg p-8 space-y-3">
          <div className="flex items-center gap-3">
            <MdCheckCircle className="text-primary text-xl flex-shrink-0" />
            <p className="text-gray-700">Bridge raw data to strategic decisions</p>
          </div>
          <div className="flex items-center gap-3">
            <MdCheckCircle className="text-primary text-xl flex-shrink-0" />
            <p className="text-gray-700">Automation, visual dashboard with experts</p>
          </div>
          <div className="flex items-center gap-3">
            <MdCheckCircle className="text-primary text-xl flex-shrink-0" />
            <p className="text-gray-700">Designed for real business outcomes- revenue, retention, inventory cost</p>
          </div>
        </div>
      </div>
    </section>
  )
}
