import React from 'react';
import { Button } from 'primereact/button';

const successClass = "btn-gradient-outline text-transparent bg-clip-text bg-gradient-to-r from-green-400 via-green-500 to-green-600 border border-gradient-to-r border-green-400 hover:border-green-500 rounded-base font-medium text-sm px-5 py-3.5 text-center leading-5 shadow-sm hover:shadow-green-500/50 focus:outline-none focus:ring-4 focus:ring-green-300 dark:focus:ring-green-800 transform transition-all duration-200 ease-out hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const warningClass = "btn-gradient-outline text-transparent bg-clip-text bg-gradient-to-r from-yellow-400 via-yellow-500 to-yellow-600 border border-gradient-to-r border-yellow-400 hover:border-yellow-500 rounded-base font-medium text-sm px-5 py-3.5 text-center leading-5 shadow-sm hover:shadow-yellow-500/50 focus:outline-none focus:ring-4 focus:ring-yellow-300 dark:focus:ring-yellow-800 transform transition-all duration-200 ease-out hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const infoClass = "btn-gradient-outline text-transparent bg-clip-text bg-gradient-to-r from-blue-400 via-blue-500 to-blue-600 border border-gradient-to-r border-blue-400 hover:border-blue-500 rounded-base font-medium text-sm px-5 py-3.5 text-center leading-5 shadow-sm hover:shadow-blue-500/50 focus:outline-none focus:ring-4 focus:ring-blue-300 dark:focus:ring-blue-800 transform transition-all duration-200 ease-out hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const dangerClass = "btn-gradient-outline text-transparent bg-clip-text bg-gradient-to-r from-red-400 via-red-500 to-red-600 border border-gradient-to-r border-red-400 hover:border-red-500 rounded-base font-medium text-sm px-5 py-3.5 text-center leading-5 shadow-sm hover:shadow-red-500/50 focus:outline-none focus:ring-4 focus:ring-red-300 dark:focus:ring-red-800 transform transition-all duration-200 ease-out hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const helpClass = "btn-gradient-outline text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-purple-500 to-purple-600 border border-gradient-to-r border-purple-400 hover:border-purple-500 rounded-base font-medium text-sm px-5 py-3.5 text-center leading-5 shadow-sm hover:shadow-purple-500/50 focus:outline-none focus:ring-4 focus:ring-purple-300 dark:focus:ring-purple-800 transform transition-all duration-200 ease-out hover:-translate-y-1 active:translate-y-0 active:shadow-sm"

const spinnerColors = {
  success: 'text-green-500',
  warning: 'text-yellow-500',
  info: 'text-blue-500',
  danger: 'text-red-500',
  help: 'text-purple-500',
  default: 'text-emerald-400',
};


const SecondaryButton = ({
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
      onClick={onClick}
      disabled={disabled}
      loading={loading}
      loadingIcon={
        <i className={`pi pi-spin pi-spinner mr-2 ${ 
          success ? spinnerColors.success :
          warning ? spinnerColors.warning :
          info ? spinnerColors.info :
          danger ? spinnerColors.danger :
          help ? spinnerColors.help :
          spinnerColors.default
        }`} />
      }
      className={`${
        success ? successClass : ''
      } ${warning ? warningClass : ''} ${info ? infoClass : ''} ${
        danger ? dangerClass : ''
      } ${help ? helpClass : ''} ${
        !success && !warning && !info && !danger && !help ? 'btn-secondary' : ''
      } ${className}`}
      {...props}
    >
      <span className="btn-text">
        {icon && <i className={icon + " mr-2"}></i>}
        {label}
      </span>
    </Button>
  );
};

export default SecondaryButton;