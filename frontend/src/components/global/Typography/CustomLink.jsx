import React from 'react';

const CustomLink = ({ 
  children, 
  href, 
  onClick, 
  className = '',
  ...props 
}) => {
  return (
    <a
      href={href}
      onClick={onClick}
      className={`custom-link ${className}`}
      {... props}
    >
      {children}
    </a>
  );
};

export default CustomLink;