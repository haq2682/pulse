import { useState } from "react"
import { MdMenu, MdClose } from "react-icons/md"

export default function Navigation() {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <nav className="bg-white shadow-sm sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo */}
          <div className="flex-shrink-0">
            <span className="text-xl font-bold text-primary">Pulse Analytics</span>
          </div>

          {/* Desktop Menu */}
          <div className="hidden md:flex items-center space-x-8">
            <a href="#" className="text-gray-700 hover:text-primary transition">
              Home
            </a>
            <a href="#" className="text-gray-700 hover:text-primary transition">
              Terms of Service
            </a>
            <a href="#" className="text-gray-700 hover:text-primary transition">
              Privacy Policy
            </a>
            <button className="bg-accent text-white px-6 py-2 rounded-lg hover:bg-accent/90 transition">
              Get Started
            </button>
          </div>

          {/* Mobile Menu Button */}
          <div className="md:hidden">
            <button onClick={() => setIsOpen(!isOpen)} className="text-gray-700 hover:text-primary">
              {isOpen ? <MdClose size={24} /> : <MdMenu size={24} />}
            </button>
          </div>
        </div>

        {/* Mobile Menu */}
        {isOpen && (
          <div className="md:hidden pb-4 space-y-3">
            <a href="#" className="block text-gray-700 hover:text-primary">
              Home
            </a>
            <a href="#" className="block text-gray-700 hover:text-primary">
              Terms of Service
            </a>
            <a href="#" className="block text-gray-700 hover:text-primary">
              Privacy Policy
            </a>
            <button className="w-full bg-accent text-white px-6 py-2 rounded-lg hover:bg-accent/90 transition">
              Get Started
            </button>
          </div>
        )}
      </div>
    </nav>
  )
}
