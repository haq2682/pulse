import React from 'react';
import { Button } from 'primereact/button';

const SecondaryButton = ({
  label,
  icon,
  onClick,
  disabled = false,
  loading = false,
  className = '',
  ...props
}) => {
  return (
    <Button
      onClick={onClick}
      disabled={disabled}
      loading={loading}
      className={`btn-secondary ${className}`}
      {...props}
    >
      <span className="btn-text">
        {icon && <i className={icon}></i>}
        {label}
      </span>
    </Button>
  );
};

export default SecondaryButton;