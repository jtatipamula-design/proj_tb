// Rebuild static/app.css after adding Tailwind classes to templates (see README).
module.exports = {
    content: ['./templates/**/*.html'],
    darkMode: 'class',
    theme: {
        extend: {
            colors: {
                obsidian: '#212123',
                bone: '#fafafa',
                graphite: '#4a4a4a',
                carbon: '#000000',
                slate: '#5c5c5c',
                mist: '#a7a7a7',
                ash: '#c4c4c4',
                fog: '#e6e6e6',
                onyx: '#151515',
                steel: '#7f7f7f',
                iris: '#555dff',
                lagoon: '#03cbbc',
                cobalt: '#4389e6',
                brand: { 50: '#f5f3ff', 500: '#555dff', 600: '#434ce6' }
            },
            fontFamily: {
                reckless: ['Lora', 'Playfair Display', 'Georgia', 'serif'],
                ui: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
                mono: ['JetBrains Mono', 'monospace']
            }
        }
    }
}
