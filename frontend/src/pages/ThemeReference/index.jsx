import React, { useState } from 'react';
import { InputText } from 'primereact/inputtext';
import { InputTextarea } from 'primereact/inputtextarea';
import { Dropdown } from 'primereact/dropdown';
import { Checkbox } from 'primereact/checkbox';
import { RadioButton } from 'primereact/radiobutton';
import { Calendar } from 'primereact/calendar';
import { InputSwitch } from 'primereact/inputswitch';
import { Slider } from 'primereact/slider';
import { PrimaryButton, SecondaryButton } from '@/components/global/Button';
import { Heading, Text } from '@/components/global/Typography';
import { Knob } from 'primereact/knob';

const ThemeReference = () => {
    const [inputValue, setInputValue] = useState('');
    const [selectedCity, setSelectedCity] = useState(null);
    const [checked, setChecked] = useState(false);
    const [radioValue, setRadioValue] = useState(null);
    const [date, setDate] = useState(null);
    const [switchValue, setSwitchValue] = useState(false);
    const [sliderValue, setSliderValue] = useState(50);

    const cities = [
        { name: 'New York', code: 'NY' },
        { name: 'Rome', code: 'RM' },
        { name: 'London', code: 'LDN' },
        { name: 'Paris', code: 'PRS' }
    ];

    return (
        <div className="min-h-screen bg-gradient-primary p-8">
            <div className="max-w-4xl mx-auto bg-white rounded-lg p-8 shadow-lg">
                <Heading level={1} className="text-4xl mb-8">
                    PrimeReact Components Demo
                </Heading>

                {/* Input Text */}
                <div className="mb-6">
                    <label htmlFor="input" className="block mb-2 font-semibold text-[var(--color-text-primary)]">
                        Input Text
                    </label>
                    <InputText
                        id="input"
                        value={inputValue}
                        onChange={(e) => setInputValue(e.target.value)}
                        placeholder="Type something..."
                        className="w-full text-black"
                    />
                </div>

                {/* Textarea */}
                <div className="mb-6">
                    <label htmlFor="textarea" className="block mb-2 font-semibold text-[var(--color-text-primary)]">
                        Textarea
                    </label>
                    <InputTextarea
                        id="textarea"
                        rows={5}
                        cols={30}
                        placeholder="Your message..."
                        className="w-full"
                    />
                </div>

                {/* Dropdown */}
                <div className="mb-6">
                    <label htmlFor="dropdown" className="block mb-2 font-semibold text-[var(--color-text-primary)]">
                        Dropdown
                    </label>
                    <Dropdown
                        id="dropdown"
                        value={selectedCity}
                        onChange={(e) => setSelectedCity(e.value)}
                        options={cities}
                        optionLabel="name"
                        placeholder="Select a City"
                        className="w-full"
                    />
                </div>

                {/* Checkbox */}
                <div className="mb-6 flex items-center">
                    <Checkbox
                        inputId="checkbox"
                        checked={checked}
                        onChange={(e) => setChecked(e.checked)}
                    />
                    <label htmlFor="checkbox" className="ml-2 font-semibold text-[var(--color-text-primary)]">
                        I agree to the terms and conditions
                    </label>
                </div>

                {/* Radio Buttons */}
                <div className="mb-6">
                    <Text className="block mb-2 font-semibold">Choose an option:</Text>
                    <div className="flex gap-4">
                        <div className="flex items-center">
                            <RadioButton
                                inputId="option1"
                                name="option"
                                value="Option 1"
                                onChange={(e) => setRadioValue(e.value)}
                                checked={radioValue === 'Option 1'}
                            />
                            <label htmlFor="option1" className="ml-2 text-[var(--color-text-primary)]">Option 1</label>
                        </div>
                        <div className="flex items-center">
                            <RadioButton
                                inputId="option2"
                                name="option"
                                value="Option 2"
                                onChange={(e) => setRadioValue(e.value)}
                                checked={radioValue === 'Option 2'}
                            />
                            <label htmlFor="option2" className="ml-2 text-[var(--color-text-primary)]">Option 2</label>
                        </div>
                    </div>
                </div>

                {/* Calendar */}
                <div className="mb-6">
                    <label htmlFor="calendar" className="block mb-2 font-semibold text-[var(--color-text-primary)]">
                        Select Date
                    </label>
                    <Calendar
                        id="calendar"
                        value={date}
                        onChange={(e) => setDate(e.value)}
                        showIcon
                        className="w-full"
                    />
                </div>

                {/* Switch */}
                <div className="mb-6 flex items-center">
                    <InputSwitch
                        checked={switchValue}
                        onChange={(e) => setSwitchValue(e.value)}
                    />
                    <label className="ml-3 font-semibold text-[var(--color-text-primary)]">
                        Enable notifications
                    </label>
                </div>

                {/* Slider */}
                <div className="mb-6">
                    <label className="block mb-2 font-semibold text-[var(--color-text-primary)]">
                        Volume: {sliderValue}
                    </label>
                    <Slider
                        value={sliderValue}
                        onChange={(e) => setSliderValue(e.value)}
                        className="w-full"
                    />
                </div>

                {/* Buttons */}
                <div className="flex gap-4 mt-8">
                    <PrimaryButton
                        label="Submit"
                        icon="pi pi-check"
                        onClick={() => console.log('Form submitted')}
                    />
                    <SecondaryButton
                        label="Cancel"
                        icon="pi pi-times"
                        onClick={() => console.log('Cancelled')}
                    />
                </div>
            </div>
        </div>
    );
};

export default ThemeReference;