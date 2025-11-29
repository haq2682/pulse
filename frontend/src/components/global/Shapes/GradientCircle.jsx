import React from 'react';

const GradientCircle = ({ 
  size = 100, 
  className = '',
  style = {},
  ...props 
}) => {
  return (
    <div
      className={`shape-gradient rounded-full ${className}`}
      style={{
        width: `${size}px`,
        height: `${size}px`,
        ...style
      }}
      {...props}
    />
  );
};

export default GradientCircle;