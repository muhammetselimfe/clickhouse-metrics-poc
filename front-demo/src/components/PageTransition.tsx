import type { ReactNode } from 'react';

interface PageTransitionProps {
  children: ReactNode;
}

function PageTransition({ children }: PageTransitionProps) {
  return <>{children}</>;
}

export default PageTransition;

