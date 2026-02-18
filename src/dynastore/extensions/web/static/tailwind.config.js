/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./website/*.html", "./dashboard/*.html"],
  theme: {
    extend: {
      colors: {
        dyna: {
          dark: '#0f172a',
          darker: '#020617',
          primary: '#3b82f6',
          secondary: '#64748b',
          accent: '#0ea5e9',
          success: '#10b981',
          warning: '#f59e0b',
          purple: '#8b5cf6',
          danger: '#ef4444'
        }
      },
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
        mono: ['Fira Code', 'monospace'],
      },
      backgroundImage: {
        'grid-pattern': "linear-gradient(to right, #1e293b 1px, transparent 1px), linear-gradient(to bottom, #1e293b 1px, transparent 1px)",
      }
    }
  },
  plugins: [],
}
