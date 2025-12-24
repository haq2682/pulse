import React from 'react';

const GradientLine = ({ 
  width = 100, 
  height = 4,
  vertical = false,
  className = '',
  style = {},
  ... props 
}) => {
  return (
    <div
      className={`shape-gradient ${className}`}
      style={{
        width: vertical ? `${height}px` : `${width}px`,
        height: vertical ?  `${width}px` : `${height}px`,
        ... style
      }}
      {...props}
    />
  );
};

export default GradientLine;