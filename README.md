# Real-Time Trading App with Deephaven.io

A comprehensive real-time trading application built with Python, Deephaven.io, and modern web technologies.

## Features

### 🔄 Real-Time Data
- **Live Price Updates**: Ticking prices for major stocks (AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, NFLX)
- **Bid/Ask Spreads**: Real-time market depth simulation
- **Volume Tracking**: Live trading volume updates
- **Timestamp Precision**: Millisecond-accurate time stamps

### 📊 Risk Analytics
- **Portfolio P&L**: Real-time profit and loss calculations
- **Market Value**: Current portfolio valuation
- **Options Greeks**: Delta, Gamma, Theta, Vega calculations
- **Position Tracking**: Real-time position monitoring

### 🎯 User Interface
- **Grid Display**: Professional AG-Grid components for data visualization
- **Real-Time Updates**: WebSocket-based live data streaming
- **Responsive Design**: Works on desktop and mobile devices
- **Color Coding**: Green for positive, red for negative values

### 🏗️ Technology Stack
- **Backend**: Python with Deephaven.io
- **Data Processing**: Pandas, NumPy
- **Web Framework**: Flask with Socket.IO
- **Frontend**: HTML5, Tailwind CSS, AG-Grid
- **Real-Time Communication**: WebSockets

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Application
```bash
python app.py
```

### 3. Access the Dashboard
Open your browser and navigate to: `http://localhost:8080`

## Architecture

### Backend Components

#### `TradingApp` Class
- **Market Data Simulation**: Generates realistic price movements
- **Risk Calculations**: Real-time Greek calculations using Black-Scholes
- **Deephaven Integration**: Manages real-time tables
- **WebSocket Server**: Emits live updates to clients

#### Data Flow
1. Market data simulation generates price updates
2. Risk metrics calculated in real-time
3. Deephaven tables store and manage data
4. WebSocket emissions update UI instantly

### Frontend Components

#### Grid Components
- **Price Grid**: Shows live market data with bid/ask spreads
- **Risk Grid**: Displays portfolio risk metrics and Greeks
- **Portfolio Overview**: Key performance indicators

#### Real-Time Features
- **Auto-updating**: Grids refresh automatically
- **Visual Feedback**: Flash animations for price changes
- **Connection Status**: Live connection monitoring

## Configuration

### Customizing Symbols
Edit the `symbols` list in `app.py`:
```python
self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
```

### Adjusting Update Frequency
Modify the sleep duration in `simulate_market_data()`:
```python
time.sleep(0.1)  # 100ms updates
```

### Risk Parameters
Tune the Black-Scholes parameters in `calculate_greeks()`:
```python
sigma = 0.25  # Volatility
T = time_to_expiry  # Time to expiry
r = 0.05  # Risk-free rate
```

## API Endpoints

### REST Endpoints
- `GET /` - Main dashboard
- `GET /api/prices` - Current price data
- `GET /api/risk` - Current risk metrics

### WebSocket Events
- `price_update` - Live price data
- `risk_update` - Live risk metrics
- `portfolio_update` - Portfolio overview

## Development

### Project Structure
```
windsurf-project/
├── app.py                 # Main application
├── requirements.txt       # Dependencies
├── templates/
│   └── index.html        # Web dashboard
└── README.md             # Documentation
```

### Extending the App

#### Adding New Metrics
1. Update the table schemas in `init_deephaven_tables()`
2. Add calculation logic in `simulate_market_data()`
3. Update frontend grid column definitions

#### Integrating Real Data Sources
Replace the simulation with real market data:
```python
# Example: Replace with real data feed
def get_real_market_data(self):
    # Connect to your data provider
    # Return price updates in same format
    pass
```

## Performance

### Optimization Features
- **Memory Management**: Tables limited to 1000 rows
- **Efficient Updates**: Only changed data transmitted
- **Async Processing**: Non-blocking data simulation
- **Client-Side Caching**: Grids manage data efficiently

### Scaling Considerations
- **Horizontal Scaling**: Multiple app instances behind load balancer
- **Database Integration**: Persist data to external database
- **Message Queue**: Use Redis/Kafka for high-throughput scenarios

## Security Notes

⚠️ **Development Use Only**
- This is a demonstration application
- Market data is simulated
- Risk calculations are simplified
- Not suitable for production trading

## Troubleshooting

### Common Issues

#### Port Conflicts
```bash
# Change port in app.py
trading_app.run(port=8081)  # Use different port
```

#### Dependency Issues
```bash
# Install specific Deephaven version
pip install deephaven-core==0.32.0
```

#### Connection Problems
- Check firewall settings
- Verify WebSocket support
- Ensure browser compatibility

## Contributing

### Development Setup
1. Clone the repository
2. Install dependencies
3. Run the development server
4. Make changes and test

### Code Style
- Follow PEP 8 for Python
- Use semantic HTML5
- Implement responsive design
- Add comprehensive comments

## License

MIT License - feel free to use and modify for your projects.

## Support

For questions and issues:
1. Check the troubleshooting section
2. Review the code comments
3. Test with different browsers
4. Verify Deephaven installation

---

**Built with ❤️ using Deephaven.io and modern web technologies**
