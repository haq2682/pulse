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
      // FIX: Hide the label when loading. 
      // This removes the text, allowing the spinner to automatically center.
      label={loading ? null : label} 
      
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