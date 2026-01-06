import React from 'react';

const Breadcrumb = ({ items }) => {
    return (
        <nav className="flex items-center gap-2 text-sm md:text-base flex-wrap">
            {items.map((item, index) => (
                <React.Fragment key={index}>
                    <span
                        className={`
                            font-medium transition-colors
                            ${item.active
                                ? 'text-[var(--color-g1)] font-semibold'
                                : 'text-gray-500'
                            }
                            ${item.clickable ? 'cursor-pointer hover:text-[var(--color-g2)]' : ''}
                        `}
                        onClick={item.onClick}
                    >
                        {item.label}
                    </span>
                    {index < items.length - 1 && (
                        <span className="text-gray-400 font-medium">{'>'}</span>
                    )}
                </React.Fragment>
            ))}
        </nav>
    );
};

export default Breadcrumb;