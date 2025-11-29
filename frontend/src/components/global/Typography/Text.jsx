import React from 'react';

const Text = ({ 
  children, 
  className = '',
  ...props 
}) => {
  return (
    <p className={`text-[var(--color-text-primary)] ${className}`} {...props}>
      {children}
    </p>
  );
};

export default Text;