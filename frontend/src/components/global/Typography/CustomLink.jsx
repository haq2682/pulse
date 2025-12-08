import React from 'react';

const CustomLink = ({
  children,
  href,
  onClick,
  className = '',
  white = false,
  gradient = true,
  ...props
}) => {
  const getClasses = () => {
    let classes = 'font-bold ';

    if (white) {
      classes += 'text-white ';
    } else if (gradient) {
      classes += 'gradient-text heading-shadow custom-link';
    } else {
      classes += 'text-[var(--color-g1)] ';
    }

    return classes + className;
  };
  return (
    <a
      href={href}
      onClick={onClick}
      className={`${getClasses()}`}
      style={{ textShadow: '1px 1px 3px rgba(0, 0, 0, 0.2)' }}
      {...props}
    >
      {children}
    </a>
  );
};

export default CustomLink;