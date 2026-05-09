import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0a0a0a",
        surface: "#0f0f0f",
        "surface-card": "#141414",
        border: "rgba(255,255,255,0.08)",
      },
      fontSize: {
        "2xs": ["10px", "1.2"],
        "h1-title": ["56px", "1.05"],
      },
      fontFamily: {
        sans: ["var(--font-geist-sans)", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};

export default config;
