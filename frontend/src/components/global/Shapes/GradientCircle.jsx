import React from 'react';

const GradientCircle = ({
  size = 100,
  className = '',
  style = {},
  children,
  ...props
}) => {
  return (
    <div
      className={`shape-gradient rounded-full ${className}`}
      style={{
        width: `${size}px`,
        height: `${size}px`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        position: 'relative',
        ...style
      }}
      {...props}
    >
      {children}
    </div>
  );
};

export default GradientCircle;