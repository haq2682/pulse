import { useEffect } from 'react';

export default function usePageTitle(title) {
    useEffect(() => {
        const base = "Pulse";
        document.title = title ? `${title} | ${base}` : base;
    }, [title]);
}