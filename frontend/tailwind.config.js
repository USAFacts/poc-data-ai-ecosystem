/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        navy: {
          50: '#F0F3F9',
          100: '#D6DDE9',
          200: '#A8B5CD',
          500: '#3D5A80',
          700: '#1B2A4A',
          800: '#142038',
          900: '#0A3161',
        },
        usared: {
          DEFAULT: '#B31942',
          light: '#FDE8ED',
        },
      },
    },
  },
  plugins: [],
}
