import React from 'react';

const Text = ({
  children,
  className = '',
  ...props
}) => {
  return (
    <p className={`text-[var(--color-text-primary)] ${className}`} {...props} style={{ textShadow: '1px 1px 3px rgba(0, 0, 0, 0.2)' }}>
      {children}
    </p>
  );
};

export default Text;