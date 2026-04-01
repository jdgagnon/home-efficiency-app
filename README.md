# Home Thermal Efficiency Analytics

Empirical diagnostic modeling of your home's envelope using Nest thermostat telemetry and historical weather data.

This application helps homeowners quantify the thermal impact of home interventions (like roof replacements or insulation upgrades) and optimizes HVAC schedules for the best balance between cost and comfort.

## 🚀 Features

- **Nest Takeout Integration**: Directly upload your `Nest Takeout` .zip file to analyze HVAC runtime patterns.
- **Weather-Aware Modeling**: Automatically fetches historical local weather data (temperature, wind speed) based on your Zip Code.
- **Efficiency Diagnostic**: Calculates the "Leak Ratio" (Heating Hours / Temperature Delta) to measure the physical integrity of your home's thermal envelope.
- **Statistical Significance**: Uses Welch's t-test to determine if changes in efficiency are statistically significant.
- **Financial Projections**: Estimates the impact of efficiency changes on your monthly utility bills.
- **Pareto Schedule Optimization**: Generates a cost-vs-comfort frontier to recommend the most energy-efficient thermostat setpoints for your specific home.

## 🛠️ Technology Stack

### Backend
- **Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Python)
- **Data Science**: Pandas, NumPy, SciPy, Statsmodels
- **Weather Data**: [Meteostat](https://meteostat.net/)
- **Deployment**: Dockerized for high portability

### Frontend
- **Framework**: [React](https://reactjs.org/) + [Vite](https://vitejs.dev/)
- **Visualizations**: [Recharts](https://recharts.org/)
- **Styling**: Modern Glassmorphism (Vanilla CSS)
- **Deployment**: Optimized for Netlify

## 📁 Project Structure

```text
├── backend/
│   ├── main.py              # FastAPI entry point & API routes
│   ├── home_efficiency.py   # Statistical modeling & data processing
│   ├── requirements.txt     # Python dependencies
│   └── Dockerfile           # Backend container configuration
├── frontend/
│   ├── src/                 # React components & dashboard logic
│   ├── package.json         # Frontend dependencies
│   └── netlify.toml         # Netlify deployment configuration
└── README.md
```

## 🌍 Deployment

The application is designed for a hybrid hosting model:

1.  **Frontend**: Deploy the `frontend/` directory to **Netlify**.
2.  **Backend**: Deploy the `backend/` directory to a persistent container host like **Render** or **Railway**.
3.  **Connection**: Set the `VITE_API_URL` environment variable in Netlify to point to your deployed backend.

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).
