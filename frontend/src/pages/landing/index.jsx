import React from 'react';
import HeroBackground from '@/assets/hero-background.png';
import Illustration1 from '@/assets/illustration1.png';
import ProcedureIllustration1 from '@/assets/procedure-illustration-1.png';
import ProcedureIllustration2 from '@/assets/procedure-illustration-2.png';
import BenefitsIllustration from '@/assets/benefits-illustration.png';
import PreviewBackground from '@/assets/preview-background.png';
import {
    PiChartLineUpBold,
    PiPackageBold,
    PiChartLineDownBold,
    PiUsersFourBold,
    PiArrowsClockwiseBold,
    PiMapPinBold,
    PiStackBold,
    PiCurrencyDollarBold
} from 'react-icons/pi';
import {
    MdCloudUpload,
    MdStorage,
    MdAutorenew,
    MdAutoGraph,
    MdPsychology,
    MdDashboard
} from 'react-icons/md';
import {
    FaStar,
    FaChartLine,
    FaMoneyBillWave,
    FaLightbulb,
    FaNetworkWired
} from 'react-icons/fa';
import { FiCheck, FiTrendingUp, FiBarChart2 } from 'react-icons/fi';
import PrimaryButton from '@/components/global/Button/PrimaryButton';
import SecondaryButton from '@/components/global/Button/SecondaryButton';
import Heading from '@/components/global/Typography/Heading';
import Text from '@/components/global/Typography/Text';
import CustomLink from '@/components/global/Typography/CustomLink';
import GradientLine from '@/components/global/Shapes/GradientLine';
import GradientCircle from '@/components/global/Shapes/GradientCircle';

const Landing = () => {
    const features = [
        {
            icon: PiChartLineUpBold,
            title: 'Revenue & Growth Trends',
            description: 'Track sales over time with targets, seasonality, and anomalies.'
        },
        {
            icon: PiPackageBold,
            title: 'Product & Category Performance',
            description: 'See best-sellers by season and category with growth signals.'
        },
        {
            icon: PiChartLineDownBold,
            title: 'Demand & Sales Forecasting',
            description: 'ML-powered projections with confidence bands for planning.'
        },
        {
            icon: PiUsersFourBold,
            title: 'Customer Segmentation (RFM)',
            description: 'Champions, Loyalists, At Risk - act on segments that matter.'
        },
        {
            icon: PiArrowsClockwiseBold,
            title: 'Cohort Retention',
            description: 'Understand month-over-month retention and loyalty.'
        },
        {
            icon: PiMapPinBold,
            title: 'Geographic Performance',
            description: 'Regional sales patterns to guide campaigns and logistics.'
        },
        {
            icon: PiStackBold,
            title: 'Inventory Optimization',
            description: 'Low-stock alerts and demand indicators to prevent stockouts.'
        },
        {
            icon: PiCurrencyDollarBold,
            title: 'CLV & AOV',
            description: 'Track lifetime value and average order value to grow profitably.'
        }
    ];

    const benefits = [
        'Bridge raw data to strategic decisions',
        'Centralized, visual dashboard with exports',
        'Designed for real business outcomes'
    ];

    const procedureSteps = [
        {
            icon: MdCloudUpload,
            title: 'Ingestion & Uploading',
            description: 'Files are uploaded for batch processing or APIs/Database URI are provided for real-time processing',
            position: 'right'
        },
        {
            icon: MdStorage,
            title: 'Schema Validation & Storage',
            description: 'Ingested data is checked and missing attributes are mapped then the data is uploaded in a data lake',
            position: 'right'
        },
        {
            icon: MdAutorenew,
            title: 'Processing & Transformation',
            description: 'Robust ETL/ELT Pipelines are used to clean, process, and transform raw data which makes it ready for analysis and model training',
            position: 'right'
        },
        {
            icon: MdAutoGraph,
            title: 'Generating Analytics',
            description: 'The processed data is then analyzed and insights are generated using advanced statistical methods',
            position: 'left'
        },
        {
            icon: MdPsychology,
            title: 'Forecasting & Predictions',
            description: 'The data is used to train machine learning models that derive predictions and sales forecasting for your business',
            position: 'left'
        },
        {
            icon: MdDashboard,
            title: 'Visualization',
            description: 'Insights are presented through an intuitive web dashboard, enabling you to visualize trends, monitor KPIs, and make informed strategic decisions.',
            position: 'left'
        }
    ];

    const benefits2 = [
        {
            icon: FaStar,
            title: 'Competitive Advantage',
            description: 'Stay ahead in the market with data-driven strategies.'
        },
        {
            icon: FaChartLine,
            title: 'Optimized Operations',
            description: 'Improve inventory management, pricing strategies, and marketing campaigns.'
        },
        {
            icon: FaMoneyBillWave,
            title: 'Time & Cost Savings',
            description: 'Automate analysis and reduce manual effort.'
        },
        {
            icon: FaLightbulb,
            title: 'Informed Decisions',
            description: 'Make accurate and effective strategic choices based on reliable insights.'
        },
        {
            icon: FaNetworkWired,
            title: 'Scalable & Robust',
            description: 'Designed to handle growing data volumes and ensure continuous operation.'
        }
    ];

    return (
        <div className="landing-page">
            {/* Navigation */}
            <nav className="nav-container">
                <div className="nav-content">
                    {/* Logo */}
                    <div className="logo-container">
                        <Heading level={2} white={true} className="logo-text">
                            Pulse Analytics
                        </Heading>
                    </div>

                    {/* Navigation Links - Hidden on mobile */}
                    <div className="nav-links">
                        <CustomLink href="#about" white={true} className="nav-link">
                            About
                        </CustomLink>
                        <CustomLink href="#terms" white={true} className="nav-link">
                            Terms of Service
                        </CustomLink>
                        <CustomLink href="#privacy" white={true} className="nav-link">
                            Privacy Policy
                        </CustomLink>
                    </div>

                    {/* Log In Button */}
                    <div>
                        <SecondaryButton
                            label="Log In"
                            onClick={() => console.log('Log In clicked')}
                            className="! bg-white"
                        />
                    </div>
                </div>
            </nav>

            {/* Hero Section with Curved Bottom */}
            <section className="rounded-b-4xl hero-section">
                {/* Background Image */}
                <div className="hero-background rounded-b-4xl">
                    <img
                        src={HeroBackground}
                        alt="Background"
                        className="background-image"
                    />
                </div>

                {/* Content */}
                <div className="hero-content">
                    <div className="md:flex items-center mx-5">
                        {/* Left Content */}
                        <div className="hero-text">
                            <Heading
                                level={1}
                                white={true}
                                className="hero-heading text-center md:text-left"
                            >
                                Gain a Bird's-Eye View of your E-Commerce Business with Pulse.
                            </Heading>

                            <Text className="hero-description text-center md:text-left">
                                Our data analytics engine empowers business owners to make strategic, data-driven decisions optimize operations, and achieve competitive advantage.
                            </Text>

                            {/* CTA Buttons */}
                            <div className="flex flex-col items-center md:flex-row gap-4 m-6">
                                <SecondaryButton
                                    label="Get Started"
                                    onClick={() => console.log('Get Started clicked')}
                                    className=""
                                />

                                <PrimaryButton
                                    label="Explore Features"
                                    onClick={() => console.log('Explore Features clicked')}
                                    className=""
                                />
                            </div>
                        </div>

                        {/* Right Illustration */}
                        <div className="hero-illustration">
                            <img
                                src={Illustration1}
                                alt="Analytics Dashboard Illustration"
                                className="illustration-image object-contain drop-shadow-lg transform scale-120"
                            />
                        </div>
                    </div>
                </div>
            </section>

            {/* Features Section */}
            <section className="features-section">
                <div className="features-container">
                    <div className="features-grid">
                        {/* Feature 1 - Data Integrity */}
                        <div className="feature-card">
                            <GradientCircle size={64} className="feature-icon-circle">
                                <FiCheck className="feature-icon" />
                            </GradientCircle>
                            <Heading level={3} gradient={true} className="feature-title">
                                Data Integrity
                            </Heading>
                            <Text className="feature-description">
                                Accuracy, completeness, timeliness
                            </Text>
                        </div>

                        {/* Feature 2 - Near Real-Time */}
                        <div className="feature-card">
                            <GradientCircle size={64} className="feature-icon-circle">
                                <FiTrendingUp className="feature-icon" />
                            </GradientCircle>
                            <Heading level={3} gradient={true} className="feature-title">
                                Near Real-Time
                            </Heading>
                            <Text className="feature-description">
                                Automated refresh & alerts
                            </Text>
                        </div>

                        {/* Feature 3 - Business Outcomes */}
                        <div className="feature-card">
                            <GradientCircle size={64} className="feature-icon-circle">
                                <FiBarChart2 className="feature-icon" />
                            </GradientCircle>
                            <Heading level={3} gradient={true} className="feature-title">
                                Business Outcomes
                            </Heading>
                            <Text className="feature-description">
                                Revenue, retention, inventory cost
                            </Text>
                        </div>
                    </div>
                </div>
            </section>
            {/* Key Features Section */}
            <section className="key-features-section">
                <div className="key-features-container">
                    {/* Section Header */}
                    <div className="section-header">
                        <Heading level={2} gradient={true} className="section-title">
                            What you get with Pulse Analytics
                        </Heading>
                        <Text className="section-subtitle">
                            Key Features
                        </Text>
                        <GradientLine width={80} height={4} className="title-underline" />
                    </div>

                    {/* Section Description */}
                    <Text className="section-description">
                        A complete analytics and AI layer for e-commerce operators. Clear, visual, and truly actionable.
                    </Text>

                    {/* Features Grid */}
                    <div className="key-features-grid">
                        {features.map((feature, index) => {
                            const IconComponent = feature.icon;
                            return (
                                <div key={index} className="key-feature-card">
                                    <div className="key-feature-header">
                                        <GradientCircle size={40} className="key-feature-icon-circle">
                                            <IconComponent className="key-feature-icon" />
                                        </GradientCircle>
                                        <Heading level={4} gradient={true} className="key-feature-title">
                                            {feature.title}
                                        </Heading>
                                    </div>
                                    <Text className="key-feature-description">
                                        {feature.description}
                                    </Text>
                                </div>
                            );
                        })}
                    </div>

                    {/* Benefits List */}
                    <div className="benefits-list mx-auto w-fit">
                        {benefits.map((benefit, index) => (
                            <div key={index} className="benefit-item">
                                <GradientCircle size={28} className="benefit-check-circle">
                                    <FiCheck className="benefit-check-icon" />
                                </GradientCircle>
                                <Text className="benefit-text">
                                    {benefit}
                                </Text>
                            </div>
                        ))}
                    </div>
                </div>
            </section>
            {/* How It Works Section */}
            <section className="how-it-works-section">
                <div className="how-it-works-container">
                    {/* Section Header */}
                    <div className="section-header">
                        <Heading level={2} gradient={true} className="section-title">
                            How it works
                        </Heading>
                        <Text className="section-subtitle">
                            Procedure
                        </Text>
                        <GradientLine width={80} height={4} className="title-underline" />
                    </div>

                    {/* Section Description */}
                    <Text className="section-description">
                        A robust pipeline across ingestion, storage, transformation, analytics, ML, and visualization.
                    </Text>

                    {/* Timeline Container */}
                    <div className="timeline-container">
                        {/* Left Illustration */}
                        <div className="timeline-illustration timeline-illustration-left">
                            <img
                                src={ProcedureIllustration1}
                                alt="Data Processing Illustration"
                                className="procedure-image"
                            />
                        </div>

                        {/* Timeline */}
                        <div className="timeline">
                            {procedureSteps.map((step, index) => {
                                const IconComponent = step.icon;
                                return (
                                    <div
                                        key={index}
                                        className={`timeline-item ${step.position === 'left' ? 'timeline-item-left' : 'timeline-item-right'}`}
                                    >
                                        {/* Timeline Node */}
                                        <div className="timeline-node">
                                            <GradientCircle size={48} className="timeline-icon-circle">
                                                <IconComponent className="timeline-icon" />
                                            </GradientCircle>
                                            {index < procedureSteps.length - 1 && (
                                                <div className="timeline-line shape-gradient-linear"></div>
                                            )}
                                        </div>

                                        {/* Timeline Content */}
                                        <div className={`timeline-content ${step.position === 'left' ? 'timeline-content-left' : 'timeline-content-right'}`}>
                                            <Heading level={4} gradient={true} className="timeline-title">
                                                {step.title}
                                            </Heading>
                                            <Text className="timeline-description">
                                                {step.description}
                                            </Text>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>

                        {/* Right Illustration */}
                        <div className="timeline-illustration timeline-illustration-right">
                            <img
                                src={ProcedureIllustration2}
                                alt="Analytics Dashboard Illustration"
                                className="procedure-image"
                            />
                        </div>
                    </div>

                    {/* Footer Note */}
                    <div className="procedure-footer">
                        <Text className="procedure-footer-text">
                            All this procedure is done using parallel and distributed computing and processing
                        </Text>
                    </div>
                </div>
            </section>
            {/* Why Pulse Analytics Section */}
            <section className="why-pulse-section">
                <div className="why-pulse-container">
                    {/* Section Header */}
                    <div className="section-header">
                        <Heading level={2} gradient={true} className="section-title">
                            Why Pulse Analytics
                        </Heading>
                        <Text className="section-subtitle">
                            Benefits
                        </Text>
                        <GradientLine width={80} height={4} className="title-underline" />
                    </div>

                    {/* Section Description */}
                    <Text className="section-description">
                        Built on modern data and web technologies for reliability, speed, and maintainability
                    </Text>

                    {/* Benefits Grid with Illustration */}
                    <div className="benefits-content">
                        {/* Benefits List */}
                        <div className="benefits-list-section">
                            {benefits2.map((benefit, index) => {
                                const IconComponent = benefit.icon;
                                return (
                                    <div key={index} className="benefit-item-large">
                                        <GradientCircle size={56} className="benefit-icon-circle">
                                            <IconComponent className="benefit-icon-large" />
                                        </GradientCircle>
                                        <div className="benefit-content">
                                            <Heading level={4} gradient={true} className="benefit-title-large">
                                                {benefit.title}
                                            </Heading>
                                            <Text className="benefit-description-large">
                                                {benefit.description}
                                            </Text>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>

                        {/* Illustration */}
                        <div className="benefits-illustration">
                            <img
                                src={BenefitsIllustration}
                                alt="Benefits Illustration"
                                className="benefits-image"
                            />
                        </div>
                    </div>
                </div>
            </section>
            {/* Dashboard Preview Section */}
            <section className="dashboard-preview-section">
                {/* Background Image */}
                <div className="dashboard-background">
                    <img
                        src={PreviewBackground}
                        alt="Background"
                        className="wavy-background-image"
                    />
                </div>

                <div className="dashboard-preview-container">
                    <div className="dashboard-content-grid">
                        {/* Left Content */}
                        <div className="dashboard-text-content">
                            <Heading level={1} white={true} className="dashboard-heading">
                                See your business at a glance
                            </Heading>

                            <Text className="dashboard-description">
                                KPIs, trends, segments, forecasts, and alerts - beautifully organized for clarity and speed. Spend time deciding, not deciphering.
                            </Text>

                            {/* Features List */}
                            <div className="dashboard-features-list">
                                <div className="dashboard-feature-item">
                                    <GradientCircle size={32} className="dashboard-check-circle">
                                        <FiCheck className="dashboard-check-icon" />
                                    </GradientCircle>
                                    <Text className="dashboard-feature-text">
                                        Revenue & AOV with seasonality and targets
                                    </Text>
                                </div>

                                <div className="dashboard-feature-item">
                                    <GradientCircle size={32} className="dashboard-check-circle">
                                        <FiCheck className="dashboard-check-icon" />
                                    </GradientCircle>
                                    <Text className="dashboard-feature-text">
                                        RFM & cohorts for lifecycle actions
                                    </Text>
                                </div>

                                <div className="dashboard-feature-item">
                                    <GradientCircle size={32} className="dashboard-check-circle">
                                        <FiCheck className="dashboard-check-icon" />
                                    </GradientCircle>
                                    <Text className="dashboard-feature-text">
                                        AI forecasting and inventory alerts
                                    </Text>
                                </div>
                            </div>

                            {/* CTA Button */}
                            <div className="dashboard-cta">
                                <SecondaryButton
                                    label="Get Started"
                                    onClick={() => console.log('Get Started clicked')}
                                />
                            </div>
                        </div>

                        {/* Right Dashboard Image */}
                        <div className="dashboard-preview-image-container">
                            <img
                                src="/dashboard-preview.png"
                                alt="Dashboard Preview"
                                className="dashboard-preview-image"
                            />
                        </div>
                    </div>
                </div>
            </section>

            {/* Footer */}
            <footer className="footer-section">
                <div className="footer-container">
                    <div className="footer-content">
                        {/* Left - Branding */}
                        <div className="footer-branding">
                            <Heading level={3} white={true} className="footer-logo">
                                Pulse Analytics
                            </Heading>
                            <Text className="footer-tagline">
                                E-Commerce Insights
                            </Text>
                        </div>

                        {/* Right - Copyright */}
                        <div className="footer-copyright">
                            <Text className="footer-copyright-text">
                                © 2025 Pulse. Built for strategic, data-driven commerce.
                            </Text>
                        </div>
                    </div>
                </div>
            </footer>
        </div>
    );
};

export default Landing;