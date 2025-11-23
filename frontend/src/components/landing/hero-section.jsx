import reactsvg from "@/assets/react.svg"

export default function HeroSection() {
  return (
    <section className="min-h-screen bg-gradient-to-br from-primary via-primary-light to-primary-lighter flex items-center justify-center relative overflow-hidden">
      {/* Background decoration */}
      <div className="absolute top-0 right-0 w-96 h-96 bg-primary-lighter/30 rounded-full blur-3xl -z-10"></div>
      <div className="absolute bottom-0 left-0 w-96 h-96 bg-primary/20 rounded-full blur-3xl -z-10"></div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 w-full">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 items-center">
          {/* Left Content */}
          <div className="text-white space-y-6">
            <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold leading-tight">
              Gain a Bird's-Eye View of your E-Commerce Business with Pulse.
            </h1>
            <p className="text-base sm:text-lg text-white/90 leading-relaxed">
              Our data analytics engine empowers business owners to make strategic decisions and achieve competitive
              advantage.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 pt-4">
              <button className="bg-accent text-white px-8 py-3 rounded-lg hover:bg-accent/90 transition font-semibold">
                Explore Features
              </button>
              <button className="border-2 border-white text-white px-8 py-3 rounded-lg hover:bg-white/10 transition font-semibold">
                Learn More
              </button>
            </div>
          </div>

          {/* Right Image */}
          <div className="flex justify-center lg:justify-end">
            <div className="relative w-full h-96 lg:h-full max-w-md lg:max-w-full">
              <img
                src={reactsvg}
                alt="Analytics Dashboard Illustration"
                className="object-contain"
              />
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
