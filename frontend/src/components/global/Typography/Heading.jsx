import React from 'react';

const Heading = ({
  children,
  level = 1,
  gradient = true,
  white = false,
  black = false,
  className = '',
  ...props
}) => {
  const Tag = `h${level}`;

  const getClasses = () => {
    let classes = 'font-bold ';

    if (white) {
      classes += 'text-white ';
    } else if (black) {
      classes += 'text-black ';
    } else if (gradient) {
      classes += 'gradient-text heading-shadow ';
    } else {
      classes += 'text-[var(--color-g2)] ';
    }

    return classes + className;
  };

  return (
    <Tag className={getClasses()} {...props} style={{ textShadow: '1px 1px 3px rgba(0, 0, 0, 0.2)' }}>
      {children}
    </Tag>
  );
};

export default Heading;