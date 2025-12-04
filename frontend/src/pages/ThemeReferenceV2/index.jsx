import { PrimaryButton, SecondaryButton } from '@/components/global/Button';
import { Heading, CustomLink, Text } from '@/components/global/Typography';
import { GradientCircle, GradientLine } from '@/components/global/Shapes';

function App() {
    return (
        <div className="min-h-screen bg-gradient-primary p-8">
            <div className="max-w-4xl mx-auto bg-white rounded-lg p-8 shadow-lg">
                {/* Headings */}
                <Heading level={1} className="mb-4 text-4xl">
                    Welcome to Custom Theme
                </Heading>

                <Heading level={2} gradient={false} white={false} className="mb-4 text-3xl">
                    This is a non-gradient heading
                </Heading>

                <Heading level={3} white={true} className="mb-6 bg-gray-800 p-4 rounded text-2xl">
                    White Heading on Dark Background
                </Heading>

                {/* Text */}
                <Text className="mb-6 text-base">
                    This is regular text content using the custom text color (#636363).
                    It's designed to be readable and consistent throughout your application.
                </Text>

                {/* Links */}
                <div className="mb-6">
                    <CustomLink href="#" className="mr-4">
                        This is a custom link
                    </CustomLink>
                    <CustomLink href="#" onClick={(e) => { e.preventDefault(); alert('Clicked! '); }}>
                        Click me!
                    </CustomLink>
                </div>

                {/* Buttons */}
                <div className="flex flex-wrap gap-4 mb-8">
                    <PrimaryButton
                        label="Primary Button"
                        icon="pi pi-check"
                        onClick={() => alert('Primary clicked!')}
                    />
                    <SecondaryButton
                        label="Secondary Button"
                        icon="pi pi-times"
                        onClick={() => alert('Secondary clicked!')}
                    />
                    <PrimaryButton
                        label="Loading..."
                        loading={true}
                    />
                    <SecondaryButton
                        label="Disabled"
                        disabled={true}
                    />
                </div>

                {/* Geometrical Shapes */}
                <div className="mb-6">
                    <Heading level={4} className="mb-4 text-xl">
                        Geometrical Shapes
                    </Heading>

                    <div className="flex items-center gap-6 mb-4 flex-wrap">
                        <GradientCircle size={60} />
                        <GradientCircle size={80} />
                        <GradientCircle size={100} />
                    </div>

                    <div className="space-y-4">
                        <GradientLine width={200} height={4} />
                        <GradientLine width={300} height={6} />
                        <GradientLine width={150} height={8} />
                    </div>

                    <div className="flex gap-4 mt-4">
                        <GradientLine width={100} height={4} vertical={true} />
                        <GradientLine width={150} height={6} vertical={true} />
                        <GradientLine width={80} height={8} vertical={true} />
                    </div>
                </div>

                {/* Additional Examples */}
                <div className="mt-8 p-6 bg-gray-50 rounded-lg">
                    <Heading level={4} className="mb-4 text-xl">
                        Using Tailwind v4 CSS Variables
                    </Heading>
                    <div className="space-y-2">
                        <div className="h-12 bg-[var(--color-g1)] rounded flex items-center justify-center text-white font-semibold">
                            Using --color-g1
                        </div>
                        <div className="h-12 bg-[var(--color-g2)] rounded flex items-center justify-center text-white font-semibold">
                            Using --color-g2
                        </div>
                        <div className="h-12 shadow-[var(--shadow-primary)] bg-white rounded flex items-center justify-center font-semibold text-[var(--color-text-primary)]">
                            Custom Shadow & Text Color
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default App;