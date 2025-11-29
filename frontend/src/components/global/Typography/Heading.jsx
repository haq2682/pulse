import React from 'react';

const Heading = ({ 
  children, 
  level = 1, 
  gradient = true, 
  white = false,
  className = '',
  ...props 
}) => {
  const Tag = `h${level}`;
  
  const getClasses = () => {
    let classes = 'font-bold ';
    
    if (white) {
      classes += 'text-white ';
    } else if (gradient) {
      classes += 'gradient-text heading-shadow ';
    } else {
      classes += 'text-[var(--color-text-primary)] ';
    }
    
    return classes + className;
  };
  
  return (
    <Tag className={getClasses()} {...props}>
      {children}
    </Tag>
  );
};

export default Heading;