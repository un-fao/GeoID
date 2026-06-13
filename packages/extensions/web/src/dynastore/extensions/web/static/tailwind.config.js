/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    // Web extension pages
    "./website/*.html",
    "./dashboard/*.html",
    "./docs.html",
    // Extension browser pages
    "../../../../../../../edr/src/dynastore/extensions/edr/static/*.html",
    "../../../../../../../stac/src/dynastore/extensions/stac/static/*.html",
    "../../../../../../../features/src/dynastore/extensions/features/static/*.html",
    "../../../../../../../coverages/src/dynastore/extensions/coverages/static/*.html",
    "../../../../../../../moving_features/src/dynastore/extensions/moving_features/static/*.html",
    "../../../../../../../assets/src/dynastore/extensions/assets/static/*.html",
    "../../../../../../../volumes/src/dynastore/extensions/volumes/static/*.html",
    "../../../../../../../tiles/src/dynastore/extensions/tiles/static/*.html",
  ],
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
