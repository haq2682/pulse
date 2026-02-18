import React from 'react';
import { Button } from 'primereact/button';

const successClass = "text-white bg-gradient-to-r from-green-400 via-green-500 to-green-600 bg-[length:200%_200%] bg-left hover:bg-right transition-all duration-500 ease-out focus:ring-4 focus:outline-none focus:ring-green-300 dark:focus:ring-green-800 shadow-md shadow-green-500/50 dark:shadow-md dark:shadow-green-800/80 font-medium rounded-base text-sm px-5 py-3.5 text-center leading-5 transform hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const warningClass = "text-white bg-gradient-to-r from-yellow-400 via-yellow-500 to-yellow-600 bg-[length:200%_200%] bg-left hover:bg-right transition-all duration-500 ease-out focus:ring-4 focus:outline-none focus:ring-yellow-300 dark:focus:ring-yellow-800 shadow-md shadow-yellow-500/50 dark:shadow-md dark:shadow-yellow-800/80 font-medium rounded-base text-sm px-5 py-3.5 text-center leading-5 transform hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const infoClass = "text-white bg-gradient-to-r from-blue-400 via-blue-500 to-blue-600 bg-[length:200%_200%] bg-left hover:bg-right transition-all duration-500 ease-out focus:ring-4 focus:outline-none focus:ring-blue-300 dark:focus:ring-blue-800 shadow-md shadow-blue-500/50 dark:shadow-md dark:shadow-blue-800/80 font-medium rounded-base text-sm px-5 py-3.5 text-center leading-5 transform hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const dangerClass = "text-white bg-gradient-to-r from-red-400 via-red-500 to-red-600 bg-[length:200%_200%] bg-left hover:bg-right transition-all duration-500 ease-out focus:ring-4 focus:outline-none focus:ring-red-300 dark:focus:ring-red-800 shadow-md shadow-red-500/50 dark:shadow-md dark:shadow-red-800/80 font-medium rounded-base text-sm px-5 py-3.5 text-center leading-5 transform hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const helpClass = "text-white bg-gradient-to-r from-purple-400 via-purple-500 to-purple-600 bg-[length:200%_200%] bg-left hover:bg-right transition-all duration-500 ease-out focus:ring-4 focus:outline-none focus:ring-purple-300 dark:focus:ring-purple-800 shadow-md shadow-purple-500/50 dark:shadow-md dark:shadow-purple-800/80 font-medium rounded-base text-sm px-5 py-3.5 text-center leading-5 transform hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

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
      
      icon={icon + " mr-2"}
      onClick={onClick}
      disabled={disabled}
      loading={loading}
      className={`${success ? successClass : ''} ${warning ? warningClass : ''} ${info ? infoClass : ''} ${danger ? dangerClass : ''} ${help ? helpClass : ''} ${!success && !warning && !info && !danger && !help ? 'btn-primary' : ''} ${className}`}
      {...props}
    />
  );
};

export default PrimaryButton;