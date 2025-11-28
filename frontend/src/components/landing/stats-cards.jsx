import { FiCheckCircle } from "react-icons/fi"
import { MdAccessTime, MdTrendingUp } from "react-icons/md"

export default function StatsCards() {
  const stats = [
    {
      icon: <MdAccessTime className="w-8 h-8" />,
      title: "Data Integrity",
      description: "Accuracy, completeness, timeliness",
    },
    {
      icon: <FiCheckCircle className="w-8 h-8" />,
      title: "Near Real-Time",
      description: "Automated refresh & alerts",
    },
    {
      icon: <MdTrendingUp className="w-8 h-8" />,
      title: "Business Outcomes",
      description: "Revenue, retention, inventory cost",
    },
  ]

  return (
    <section className="min-h-screen bg-white flex items-center justify-center py-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 w-full">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {stats.map((stat, index) => (
            <div
              key={index}
              className="flex flex-col items-center text-center space-y-4 p-8 rounded-lg hover:shadow-lg transition"
            >
              <div className="text-primary">{stat.icon}</div>
              <h3 className="text-xl font-bold text-gray-900">{stat.title}</h3>
              <p className="text-gray-600">{stat.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
