# GB-Power-Price-Diver-Spread-Radar
Day-Ahead vs Intraday vs Imbalance price dashboard for GB power.


# GB Power Price Diver Spread Radar ⚡📈

A live dashboard comparing Day-Ahead, Intraday, and Imbalance (SBP/SSP) electricity prices in the GB market.  
Tracks price spreads and volatility, and highlights what drives them using demand and wind forecasts.

## Features

- Fetches real-time data from ENTSO-E and Elexon APIs
- Visualises Day-Ahead vs Intraday vs SBP/SSP spreads
- Triggers Slack alerts when spread risk is high
- (Coming soon) Simple ML model to forecast price divergences

## Quickstart

```bash
git clone https://github.com/alkis77/GB-Power-Price-Diver-Spread-Radar.git
cd GB-Power-Price-Diver-Spread-Radar
pip install -r requirements.txt
streamlit run app.py
