import Navigation from "@/components/landing/navigation"
import HeroSection from "@/components/landing/hero-section"
import StatsCards from "@/components/landing/stats-cards"
import FeaturesGrid from "@/components/landing/features-grid"
import HowItWorks from "@/components/landing/how-it-works"
import BenefitsSection from "@/components/landing/benefits-section"
import DashboardPreview from "@/components/landing/dashboard-preview"
import Footer from "@/components/global/footer"

export default function Landing() {
    return (
        <div className="bg-background">
            <Navigation />
            <HeroSection />
            <StatsCards />
            <FeaturesGrid />
            <HowItWorks />
            <BenefitsSection />
            <DashboardPreview />
            <Footer />
        </div>
    )
}
