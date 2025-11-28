"use client"

export default function Footer() {
  return (
    <footer className="bg-gray-900 text-white py-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {/* Left Section */}
          <div className="space-y-4">
            <h3 className="text-xl font-bold text-white">Pulse Analytics</h3>
            <p className="text-gray-400">Empowering e-commerce businesses with data-driven insights.</p>
          </div>

          {/* Right Section */}
          <div className="flex flex-col sm:flex-row justify-end gap-4">
            <button className="bg-primary text-white px-6 py-2 rounded-lg hover:bg-primary/90 transition">
              Get Started
            </button>
            <button className="border border-primary text-white px-6 py-2 rounded-lg hover:bg-primary/10 transition">
              Schedule Presentation
            </button>
          </div>
        </div>

        <div className="border-t border-gray-800 mt-8 pt-8">
          <p className="text-gray-400 text-sm text-center">
            © 2025 Pulse Analytics. All rights reserved. subscription.commerce
          </p>
        </div>
      </div>
    </footer>
  )
}
