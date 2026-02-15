import React from 'react';
import { Button } from 'primereact/button';

const successClass = "text-white bg-gradient-to-r from-green-400 via-green-500 to-green-600 hover:bg-gradient-to-br focus:ring-4 focus:outline-none focus:ring-green-300 dark:focus:ring-green-800 shadow-md shadow-green-500/50 dark:shadow-md dark:shadow-green-800/80 font-medium rounded-base text-sm px-4 py-2.5 text-center leading-5"

const warningClass = "text-white bg-gradient-to-r from-yellow-400 via-yellow-500 to-yellow-600 hover:bg-gradient-to-br focus:ring-4 focus:outline-none focus:ring-yellow-300 dark:focus:ring-yellow-800 shadow-md shadow-yellow-500/50 dark:shadow-md dark:shadow-yellow-800/80 font-medium rounded-base text-sm px-4 py-2.5 text-center leading-5"

const infoClass = "text-white bg-gradient-to-r from-blue-400 via-blue-500 to-blue-600 hover:bg-gradient-to-br focus:ring-4 focus:outline-none focus:ring-blue-300 dark:focus:ring-blue-800 shadow-md shadow-blue-500/50 dark:shadow-md dark:shadow-blue-800/80 font-medium rounded-base text-sm px-4 py-2.5 text-center leading-5"

const dangerClass = "text-white bg-gradient-to-r from-red-400 via-red-500 to-red-600 hover:bg-gradient-to-br focus:ring-4 focus:outline-none focus:ring-red-300 dark:focus:ring-red-800 shadow-md shadow-red-500/50 dark:shadow-md dark:shadow-red-800/80 font-medium rounded-base text-sm px-4 py-2.5 text-center leading-5"

const helpClass = "text-white bg-gradient-to-r from-purple-400 via-purple-500 to-purple-600 hover:bg-gradient-to-br focus:ring-4 focus:outline-none focus:ring-purple-300 dark:focus:ring-purple-800 shadow-md shadow-purple-500/50 dark:shadow-md dark:shadow-purple-800/80 font-medium rounded-base text-sm px-4 py-2.5 text-center leading-5"

const PrimaryButton = ({
  label,
  icon,
  onClick,
  success = false,
  warning = false,
  info = false,
  danger = false,
  help = false,
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
      className={`${success ? successClass : ''} ${warning ? warningClass : ''} ${info ? infoClass : ''} ${danger ? dangerClass : ''} ${help ? helpClass : ''} ${!success && !warning && !info && !danger && !help ? 'btn-primary' : ''} ${className}`}
      {...props}
    />
  );
};

export default PrimaryButton;