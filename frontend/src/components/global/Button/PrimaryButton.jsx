import React from 'react';
import { Button } from 'primereact/button';

const PrimaryButton = ({
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
      label={label}
      icon={icon}
      onClick={onClick}
      disabled={disabled}
      loading={loading}
      className={`btn-primary ${className}`}
      {...props}
    />
  );
};

export default PrimaryButton;